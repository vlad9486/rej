use std::io;

use super::{
    page::{PagePtr, RawPtr},
    runtime::{PlainData, Alloc, Free, AbstractIo, AbstractViewer, Rt},
    node::{NodePage, Child, Key, M},
};

pub fn get<T>(
    view: &impl AbstractViewer,
    mut ptr: PagePtr<NodePage>,
    key: Key<'_>,
) -> Option<PagePtr<T>> {
    loop {
        let node = view.page(ptr);
        let idx = node
            .search(view, &key)
            .or_else(|idx| if node.is_leaf() { Err(()) } else { Ok(idx) })
            .ok()?;
        if idx == M {
            return None;
        }
        match node.get_child(idx)? {
            Child::Node(p) => ptr = p,
            Child::Leaf(p) => return Some(p),
        }
    }
}

pub struct It(Option<ItInner>);

pub struct ItInner {
    ptr: PagePtr<NodePage>,
    forward: bool,
    idx: u16,
}

impl ItInner {
    fn new(
        view: &impl AbstractViewer,
        head_ptr: PagePtr<NodePage>,
        forward: bool,
        table_id: u32,
        key: Option<&[u8]>,
    ) -> Option<Self> {
        let mut ptr = head_ptr;
        let mut node = view.page(ptr);
        if node.is_empty() {
            return None;
        }

        loop {
            // TODO: careful arithmetics
            let idx = key.map_or(usize::from(!forward) * node.len(), |key| {
                let key = Key {
                    table_id,
                    bytes: key.into(),
                };

                node.search(view, &key).unwrap_or_else(|idx| idx)
            }) - usize::from(!forward);
            match node.get_child::<()>(idx)? {
                Child::Node(p) => ptr = p,
                Child::Leaf(_) => {
                    return Some(ItInner {
                        ptr,
                        forward,
                        idx: idx as u16,
                    })
                }
            }
            node = view.page(ptr);
        }
    }
}

impl It {
    pub fn new(
        view: &impl AbstractViewer,
        head_ptr: PagePtr<NodePage>,
        forward: bool,
        table_id: u32,
        key: Option<&[u8]>,
    ) -> Self {
        Self(ItInner::new(view, head_ptr, forward, table_id, key))
    }

    pub fn next<T>(&mut self, view: &impl AbstractViewer) -> Option<(Vec<u8>, PagePtr<T>)> {
        let inner = self.0.as_mut()?;

        let idx = usize::from(inner.idx);
        let page = view.page(inner.ptr);
        if idx < page.len() {
            if inner.forward {
                inner.idx += 1;
            } else if inner.idx != 0 {
                inner.idx -= 1;
            } else if let Some(ptr) = page.prev {
                inner.ptr = ptr;
                inner.idx = u16::MAX;
            } else {
                self.0 = None;
            }
            match page.get_child(idx)? {
                Child::Leaf(p) => Some((page.get_key_old(view, idx).bytes.to_vec(), p)),
                _ => panic!("BUG: `ptr` should point on leaf node"),
            }
        } else {
            if !inner.forward {
                inner.idx = page.len() as u16 - 1;
            } else if let Some(ptr) = page.next {
                inner.ptr = ptr;
                inner.idx = 0;
            } else {
                self.0 = None;
            }
            // Warning: recursion
            self.next(view)
        }
    }
}

struct Level {
    ptr: PagePtr<NodePage>,
    node: NodePage,
    idx: usize,
}

fn walk(
    view: &impl AbstractViewer,
    root: PagePtr<NodePage>,
    key: &Key<'_>,
) -> (
    PagePtr<NodePage>,
    NodePage,
    Vec<Level>,
    Result<usize, usize>,
) {
    let mut stack = Vec::<Level>::with_capacity(6);
    let mut ptr = root;

    let leaf = loop {
        let node = *view.page(ptr);
        if node.is_leaf() {
            break node;
        } else {
            let idx = node.search(view, key).unwrap_or_else(|idx| idx);
            stack.push(Level { ptr, node, idx });
            ptr = node.child[idx].unwrap_or_else(|| panic!("{idx}"));
        }
    };
    let idx = leaf.search(view, key);
    (ptr, leaf, stack, idx)
}

pub fn insert<T>(
    mut rt: Rt<'_, impl Alloc, impl Free, impl AbstractIo>,
    root: PagePtr<NodePage>,
    key: Key<'_>,
) -> io::Result<(PagePtr<NodePage>, PagePtr<T>)>
where
    T: PlainData,
{
    let (mut ptr, mut leaf, mut stack, res) = walk(&rt.io.read(), root, &key);

    let idx = match res {
        Ok(idx) => {
            let meta = leaf.child[idx].expect("must be here, just find").cast();
            return Ok((root, meta));
        }
        Err(idx) => idx,
    };

    let meta = rt.alloc.alloc();

    rt.realloc(&mut ptr);

    let mut split = leaf.insert::<T>(rt.reborrow(), ptr, meta.cast(), idx, &key)?;
    rt.io.write(ptr, &leaf)?;

    while let Some(mut level) = stack.pop() {
        rt.realloc(&mut level.ptr);

        level.node.child[level.idx] = Some(ptr);
        if let Some((key, p)) = split {
            let rt = rt.reborrow();
            split = level.node.insert::<T>(rt, level.ptr, p, level.idx, &key)?;
        }

        rt.io.write(level.ptr, &level.node)?;
        ptr = level.ptr;
    }

    if let Some((key, p)) = split {
        let parent_ptr = rt.alloc.alloc();

        let mut root = NodePage::empty();
        root.append_child(ptr);
        root.insert::<T>(rt.reborrow(), parent_ptr, p, 0, &key)?;

        rt.io.write(parent_ptr, &root)?;
        ptr = parent_ptr;
    }

    Ok((ptr, meta))
}

pub fn remove<T>(
    mut rt: Rt<'_, impl Alloc, impl Free, impl AbstractIo>,
    root: PagePtr<NodePage>,
    key: Key<'_>,
) -> io::Result<(PagePtr<NodePage>, Option<PagePtr<T>>)>
where
    T: PlainData,
{
    let (mut ptr, mut leaf, mut stack, res) = walk(&rt.io.read(), root, &key);

    let idx = match res {
        Err(_) => return Ok((root, None)),
        Ok(idx) => idx,
    };

    rt.realloc(&mut ptr);
    let mut underflow = leaf.will_underflow();
    let (meta, _) = leaf.remove(rt.reborrow(), idx)?.expect("just find");
    rt.io.write(ptr, &leaf)?;

    let view = rt.io.read();
    while let Some(mut level) = stack.pop() {
        rt.realloc(&mut level.ptr);
        if underflow {
            let left = (level.idx > 0)
                .then(|| {
                    let ptr = level.node.child[level.idx - 1]?;
                    Some((*view.page(ptr), ptr)).filter(|(page, _)| !page.will_underflow())
                })
                .flatten();
            let right = (level.idx < level.node.len() - 1)
                .then(|| {
                    let ptr = level.node.child[level.idx + 1]?;
                    Some((*view.page(ptr), ptr)).filter(|(page, _)| !page.will_underflow())
                })
                .flatten();
            let donor = [left, right]
                .into_iter()
                .flatten()
                .max_by(|a, b| a.0.len().cmp(&b.0.len()));

            if let Some((donor_page, donor_ptr)) = donor {
                let _ = donor_page;
                let _ = donor_ptr;
                underflow = false;
                // TODO: handle underflow
            } else {
                // TODO: handle underflow
                underflow = level.node.will_underflow();
            }
        }
        level.node.child[level.idx] = Some(ptr);
        rt.io.write(level.ptr, &level.node)?;
        ptr = level.ptr;
    }

    Ok((ptr, Some(meta)))
}

// for debug
pub fn print(view: &impl AbstractViewer, ptr: PagePtr<NodePage>) {
    // this is sad that I cannot debug B-Tree without using already existing B-Tree
    use std::collections::BTreeMap;

    let mut nodes = BTreeMap::new();
    let mut edges = Vec::new();

    fn print_inner(
        view: &impl AbstractViewer,
        ptr: PagePtr<NodePage>,
        nodes: &mut BTreeMap<u32, String>,
        edges: &mut Vec<(u32, u32)>,
    ) {
        let page = view.page(ptr);
        let node_text = (0..(page.len() - usize::from(!page.is_leaf())))
            .map(|idx| page.get_key_old(view, idx))
            .map(|key| {
                format!(
                    "{}:{}",
                    key.table_id,
                    std::str::from_utf8(&key.bytes).unwrap()
                )
            })
            .collect::<Vec<_>>()
            .join("|");
        nodes.insert(ptr.raw_number(), format!("\"{node_text}\""));

        for n in (0..page.len()).map(|idx| page.child[idx].unwrap()) {
            edges.push((ptr.raw_number(), n.raw_number()));
            if !page.is_leaf() {
                print_inner(view, n, nodes, edges);
            }
        }
    }

    print_inner(view, ptr, &mut nodes, &mut edges);

    let edges = edges
        .into_iter()
        .map(|(b, e)| {
            format!(
                "{} -> {}",
                nodes[&b],
                nodes.get(&e).cloned().unwrap_or(e.to_string())
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    print!("digraph {{\n{edges}\n}}\n");
}

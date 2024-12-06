use std::{io, mem};

use super::{
    page::{PagePtr, RawPtr},
    runtime::{PlainData, Alloc, Free, AbstractIo, AbstractViewer, Rt},
    node::{NodePage, Child, Key, M},
};

pub fn get<T>(
    view: &impl AbstractViewer,
    mut ptr: PagePtr<NodePage>,
    table_id: u32,
    key: &[u8],
) -> Option<PagePtr<T>> {
    let key = Key {
        table_id,
        bytes: key.into(),
    };

    loop {
        let node = view.page(ptr);
        let idx = node.search(view, &key).unwrap_or_else(|idx| idx);
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
                Child::Leaf(p) => Some((page.get_key(view, idx).bytes.to_vec(), p)),
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

pub fn insert<T>(
    mut rt: Rt<'_, impl Alloc, impl Free, impl AbstractIo>,
    old_head: PagePtr<NodePage>,
    table_id: u32,
    key: &[u8],
) -> io::Result<(PagePtr<NodePage>, PagePtr<T>)>
where
    T: PlainData,
{
    let key = Key {
        table_id,
        bytes: key.into(),
    };

    let mut stack = Vec::<Level>::with_capacity(6);

    struct Level {
        ptr: PagePtr<NodePage>,
        node: NodePage,
        idx: usize,
    }

    let view = rt.io.read();
    let mut ptr = old_head;

    let mut leaf = loop {
        let node = *view.page(ptr);
        if node.is_leaf() {
            break node;
        } else {
            let idx = node.search(&view, &key).unwrap_or_else(|idx| idx);
            stack.push(Level { ptr, node, idx });
            ptr = node.child[idx].unwrap_or_else(|| panic!("{idx}"));
        }
    };

    match leaf.search(&view, &key) {
        Ok(idx) => {
            let new_head = stack.first().map(|level| level.ptr).unwrap_or(ptr);
            let meta = leaf.child[idx].expect("must be here, just find").cast();
            Ok((new_head, meta))
        }
        Err(idx) => {
            let new_child_ptr = rt.alloc.alloc::<T>().cast();

            rt.free.free(mem::replace(&mut ptr, rt.alloc.alloc()));

            let mut split = leaf.insert::<T>(rt.reborrow(), ptr, new_child_ptr, idx, &key)?;

            while let Some(mut level) = stack.pop() {
                rt.free.free(mem::replace(&mut level.ptr, rt.alloc.alloc()));

                level.node.child[level.idx] = Some(ptr);
                if let Some((key, sib_ptr)) = split {
                    split = level.node.insert::<T>(
                        rt.reborrow(),
                        level.ptr,
                        sib_ptr,
                        level.idx,
                        &key,
                    )?;
                }

                rt.io.write(level.ptr, &level.node)?;
                ptr = level.ptr;
            }

            if let Some((key, sib_ptr)) = split {
                let parent_ptr = rt.alloc.alloc();

                let mut root = NodePage::empty();
                root.append_child(ptr);
                root.insert::<T>(rt.reborrow(), parent_ptr, sib_ptr, 0, &key)?;

                rt.io.write(parent_ptr, &root)?;
                ptr = parent_ptr;
            }

            Ok((ptr, new_child_ptr.cast()))
        }
    }
}

// TODO: remove value
pub fn remove<T>(
    mut rt: Rt<'_, impl Alloc, impl Free, impl AbstractIo>,
    old_head: PagePtr<NodePage>,
    table_id: u32,
    key: &[u8],
) -> io::Result<(PagePtr<NodePage>, Option<PagePtr<T>>)> {
    let _ = (&mut rt, old_head, table_id, key);
    unimplemented!()
}

// for debug
pub fn print(view: &impl AbstractViewer, ptr: PagePtr<NodePage>) {
    // this is sad that I cannot debug B-Tree without already existing B-Tree
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
            .map(|idx| page.get_key(view, idx))
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

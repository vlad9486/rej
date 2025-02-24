use super::{
    page::{PagePtr, RawPtr},
    runtime::{PlainData, Free, AbstractIo},
    file::FileIo,
    value::MetadataPage,
    node::{Node, R},
};

pub struct EntryInner<N> {
    stack: Vec<Level<N>>,
    leaf: Level<N>,
}

struct Level<N> {
    ptr: PagePtr<N>,
    node: N,
    idx: usize,
}

impl<N> EntryInner<N>
where
    N: Copy + PlainData + Node,
{
    pub fn new(view: &FileIo, root: PagePtr<N>, key: &[u8]) -> (Self, bool) {
        let mut stack = Vec::with_capacity(6);
        let mut ptr = root;

        loop {
            let node = view.read(ptr);
            if node.is_leaf() {
                let pos = node.search(view, key);
                let occupied = pos.is_ok();
                let idx = pos.unwrap_or_else(|idx| idx);
                let leaf = Level { ptr, node, idx };
                return (EntryInner { stack, leaf }, occupied);
            } else {
                let idx = node.search(view, key).unwrap_or_else(|idx| idx);
                stack.push(Level { ptr, node, idx });
                ptr = node.child(idx).unwrap_or_else(|| panic!("{idx}"));
            }
        }
    }

    pub fn has_value(&self) -> bool {
        self.leaf.idx < self.leaf.node.len()
    }

    pub fn next(it: &mut Option<Self>, view: &impl AbstractIo) {
        let Some(this) = it else {
            return;
        };

        if this.leaf.idx + 1 < this.leaf.node.len() {
            this.leaf.idx += 1;
        } else {
            while let Some(mut current) = this.stack.pop() {
                if current.idx + 1 < current.node.len() {
                    current.idx += 1;
                    this.stack.push(current);
                    break;
                }
            }
            let Some(last) = this.stack.last() else {
                *it = None;
                return;
            };
            let mut ptr = last.node.child(last.idx).expect("must not fail");

            loop {
                let node = view.read(ptr);
                if node.is_leaf() {
                    let idx = 0;
                    this.leaf = Level { ptr, node, idx };
                    break;
                } else {
                    let idx = 0;
                    this.stack.push(Level { ptr, node, idx });
                    ptr = node.child(idx).unwrap_or_else(|| panic!("{idx}"));
                }
            }
        }
    }

    pub fn meta(&self) -> Option<PagePtr<MetadataPage>> {
        self.leaf.node.child(self.leaf.idx).map(PagePtr::cast)
    }

    pub fn set_meta(&mut self, meta: PagePtr<MetadataPage>) {
        *self.leaf.node.child_mut(self.leaf.idx) = Some(meta.cast());
    }

    pub fn key(&self, view: &FileIo) -> Vec<u8> {
        self.leaf.node.read_key(view, self.leaf.idx)
    }

    pub fn insert(
        self,
        mut rt: R<'_>,
        meta: Option<PagePtr<MetadataPage>>,
        key: &[u8],
    ) -> PagePtr<N> {
        let EntryInner {
            mut leaf,
            mut stack,
        } = self;

        leaf.node.realloc_keys(rt.reborrow());
        let mut split =
            leaf.node
                .insert(rt.reborrow(), meta.map(PagePtr::cast), leaf.idx, key, false);
        rt.set(&mut leaf.ptr, leaf.node);

        let mut ptr = leaf.ptr;
        while let Some(mut level) = stack.pop() {
            *level.node.child_mut(level.idx) = Some(ptr);
            if let Some((key, neighbor)) = split {
                level.node.realloc_keys(rt.reborrow());
                split = level
                    .node
                    .insert(rt.reborrow(), Some(neighbor), level.idx, &key, true);
            }
            rt.set(&mut level.ptr, level.node);

            ptr = level.ptr;
        }

        if let Some((key, neighbor)) = split {
            let mut root = N::empty();
            root.append_child(ptr);
            root.insert(rt.reborrow(), Some(neighbor), 0, &key, true);

            let parent_ptr = rt.create();
            *rt.mutate(parent_ptr) = root;
            ptr = parent_ptr;
        }

        ptr
    }

    pub fn remove(self, mut rt: R) -> PagePtr<N> {
        let EntryInner {
            mut leaf,
            mut stack,
        } = self;

        let mut underflow = !leaf.node.can_donate();
        leaf.node.realloc_keys(rt.reborrow());
        let (_, _) = leaf.node.remove(rt.reborrow(), leaf.idx, false);
        rt.set(&mut leaf.ptr, leaf.node);

        let mut prev = leaf.node;
        let mut ptr = leaf.ptr;

        while let Some(mut level) = stack.pop() {
            if underflow {
                level.node.realloc_keys(rt.reborrow());

                let mut left = (level.idx > 0).then(|| {
                    let ptr = level
                        .node
                        .child(level.idx - 1)
                        .expect("left neighbor always present");
                    NodeWithPtr {
                        node: rt.io.read(ptr),
                        ptr,
                    }
                });
                let mut right = (level.idx < level.node.len() - 1)
                    .then(|| {
                        level.node.child(level.idx + 1).map(|ptr| NodeWithPtr {
                            node: rt.io.read(ptr),
                            ptr,
                        })
                    })
                    .flatten();

                // for early return
                #[allow(clippy::never_loop)]
                loop {
                    if let Some(donor) = &mut left {
                        if donor.can_donate() && right.as_ref().map_or(true, |r| r.le(donor)) {
                            log::debug!("donate left");

                            donor.node.realloc_keys(rt.reborrow());
                            let (donated_ptr, donated_key) =
                                donor.node.remove(rt.reborrow(), donor.node.len() - 1, true);

                            prev.insert(rt.reborrow(), donated_ptr, 0, &donated_key, false);
                            *rt.mutate(ptr) = prev;
                            rt.set(&mut donor.ptr, donor.node);

                            *level.node.child_mut(level.idx - 1) = Some(donor.ptr);

                            let parent_key =
                                donor.node.get_key(rt.reborrow(), donor.node.len() - 1);
                            level
                                .node
                                .set_key(rt.reborrow(), level.idx - 1, &parent_key);

                            underflow = false;
                            break;
                        }
                    }

                    if let Some(donor) = &mut right {
                        if donor.can_donate() {
                            log::debug!("donate right");

                            donor.node.realloc_keys(rt.reborrow());
                            let (donated_ptr, donated_key) =
                                donor.node.remove(rt.reborrow(), 0, false);

                            prev.insert(
                                rt.reborrow(),
                                donated_ptr,
                                N::M / 2 - 1,
                                &donated_key,
                                false,
                            );
                            *rt.mutate(ptr) = prev;
                            rt.set(&mut donor.ptr, donor.node);

                            *level.node.child_mut(level.idx + 1) = Some(donor.ptr);

                            level.node.set_key(rt.reborrow(), level.idx, &donated_key);

                            underflow = false;
                            break;
                        }
                    }

                    if let Some(neighbor) = &mut left {
                        if right.as_ref().map_or(true, |r| r.gt(neighbor)) {
                            log::debug!("merge left");
                            underflow = !level.node.can_donate();
                            neighbor.node.realloc_keys(rt.reborrow());
                            level.idx -= 1;
                            let (_, key) = level.node.remove(rt.reborrow(), level.idx, false);
                            neighbor.node.merge(&prev, rt.reborrow(), &key, false);
                            prev.free(rt.reborrow());

                            rt.free.free(ptr);
                            rt.set(&mut neighbor.ptr, neighbor.node);
                            ptr = neighbor.ptr;

                            break;
                        }
                    }

                    if let Some(neighbor) = right {
                        underflow = !level.node.can_donate();
                        log::debug!("merge right");
                        let (neighbor_ptr, _) =
                            level.node.remove(rt.reborrow(), level.idx + 1, false);
                        let neighbor_ptr = neighbor_ptr.expect("must be there");
                        let key = level.node.get_key(rt.reborrow(), level.idx);
                        assert_eq!(neighbor_ptr, neighbor.ptr, "suppose to remove the neighbor");
                        let last_key = prev.merge(&neighbor.node, rt.reborrow(), &key, true);
                        level.node.set_key(rt.reborrow(), level.idx, &last_key);
                        neighbor.node.free(rt.reborrow());
                        rt.free.free(neighbor.ptr);
                        *rt.mutate(ptr) = prev;

                        break;
                    }

                    break;
                }
            }
            if level.node.len() == 1 && !level.node.is_leaf() {
                log::debug!("decrease height");
                level.node.free(rt.reborrow());
                rt.free.free(level.ptr);
            } else {
                *level.node.child_mut(level.idx) = Some(ptr);
                rt.set(&mut level.ptr, level.node);
                ptr = level.ptr;
                prev = level.node;
            }
        }

        ptr
    }
}

struct NodeWithPtr<N> {
    node: N,
    ptr: PagePtr<N>,
}

impl<N> NodeWithPtr<N>
where
    N: Node,
{
    fn can_donate(&self) -> bool {
        self.node.can_donate()
    }
}

impl<N> PartialEq for NodeWithPtr<N>
where
    N: Node,
{
    fn eq(&self, other: &Self) -> bool {
        self.node.len().eq(&other.node.len())
    }
}

impl<N> PartialOrd for NodeWithPtr<N>
where
    N: Node,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.node.len().partial_cmp(&other.node.len())
    }
}

// for debug
#[cfg(test)]
pub fn print<N, K, D>(rt: R<'_>, ptr: PagePtr<N>, k: K, old: bool)
where
    N: Copy + PlainData + Node,
    K: Fn(&[u8]) -> D,
    D: std::fmt::Display,
{
    // this is sad that I cannot debug B-Tree without using already existing B-Tree
    use std::collections::BTreeMap;

    let mut nodes = BTreeMap::new();
    let mut edges = Vec::new();

    fn print_inner<N, K, D>(
        mut rt: R<'_>,
        ptr: PagePtr<N>,
        nodes: &mut BTreeMap<u32, String>,
        edges: &mut Vec<(u32, u32)>,
        k: &K,
        old: bool,
    ) where
        N: Copy + PlainData + Node,
        K: Fn(&[u8]) -> D,
        D: std::fmt::Display,
    {
        let page = rt.io.read(ptr);
        let node_text = (0..(page.len() - usize::from(!page.is_leaf())))
            .map(|idx| {
                if old {
                    page.read_key(rt.io, idx)
                } else {
                    page.get_key(rt.reborrow(), idx)
                }
            })
            .map(|key| format!("{}", k(&key)))
            .collect::<Vec<_>>()
            .join("|");
        nodes.insert(ptr.raw_number(), format!("\"{node_text}\""));

        for n in (0..page.len()).map(|idx| page.child(idx)) {
            let child_ptr = n.map(PagePtr::raw_number).unwrap_or_default();
            edges.push((ptr.raw_number(), child_ptr));
            if !page.is_leaf() {
                print_inner(rt.reborrow(), n.expect("BUG"), nodes, edges, k, old);
            }
        }
    }

    print_inner(rt, ptr, &mut nodes, &mut edges, &k, old);

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
    log::debug!("digraph {{\n{edges}\n}}\n");
}

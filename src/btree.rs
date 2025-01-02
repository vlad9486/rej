use std::io;

use super::{
    page::{PagePtr, RawPtr},
    runtime::{Alloc, Free, AbstractIo, AbstractViewer, Rt},
    value::MetadataPage,
    node::{NodePage, Key, K},
};

pub struct EntryInner {
    stack: Vec<Level>,
    leaf: Level,
}

struct Level {
    ptr: PagePtr<NodePage>,
    node: NodePage,
    idx: usize,
}

impl EntryInner {
    pub fn new(view: &impl AbstractViewer, root: PagePtr<NodePage>, key: &Key<'_>) -> (Self, bool) {
        let mut stack = Vec::with_capacity(6);
        let mut ptr = root;

        loop {
            let node = *view.page(ptr);
            if node.is_leaf() {
                let pos = node.search(view, key);
                let occupied = pos.is_ok();
                let idx = pos.unwrap_or_else(|idx| idx);
                let leaf = Level { ptr, node, idx };
                return (EntryInner { stack, leaf }, occupied);
            } else {
                let idx = node.search(view, key).unwrap_or_else(|idx| idx);
                stack.push(Level { ptr, node, idx });
                ptr = node.child[idx].unwrap_or_else(|| panic!("{idx}"));
            }
        }
    }

    pub fn has_next(&self, _view: &impl AbstractViewer) -> bool {
        self.stack
            .iter()
            .chain(Some(&self.leaf))
            .rev()
            .find(|level| level.idx + 1 < level.node.len())
            .is_some()
    }

    pub fn next(&mut self, view: &impl AbstractViewer) {
        if self.leaf.idx + 1 < self.leaf.node.len() {
            self.leaf.idx += 1;
            return;
        } else {
            while let Some(mut current) = self.stack.pop() {
                if current.idx + 1 < current.node.len() {
                    current.idx += 1;
                    self.stack.push(current);
                    break;
                }
            }
            let last = self.stack.last().expect("check `has_next` before use");
            let mut ptr = last.node.child[last.idx].expect("check `has_next` before use");

            loop {
                let node = *view.page(ptr);
                if node.is_leaf() {
                    let idx = 0;
                    self.leaf = Level { ptr, node, idx };
                    break;
                } else {
                    let idx = 0;
                    self.stack.push(Level { ptr, node, idx });
                    ptr = node.child[idx].unwrap_or_else(|| panic!("{idx}"));
                }
            }
        }
    }

    pub fn meta(&self) -> PagePtr<MetadataPage> {
        self.leaf.node.child[self.leaf.idx]
            .expect("must be here")
            .cast()
    }

    pub fn key<'c>(&self, view: &impl AbstractViewer) -> Key<'c> {
        self.leaf.node.get_key_old(view, self.leaf.idx)
    }

    pub fn insert(
        self,
        mut rt: Rt<'_, impl Alloc, impl Free, impl AbstractIo>,
        meta: PagePtr<MetadataPage>,
        key: &Key<'_>,
    ) -> io::Result<PagePtr<NodePage>> {
        let EntryInner {
            mut leaf,
            mut stack,
        } = self;

        rt.realloc(&mut leaf.ptr);

        leaf.node.realloc_keys(rt.reborrow());
        let mut split = leaf
            .node
            .insert(rt.reborrow(), meta.cast(), leaf.idx, key, false);
        rt.io.write(leaf.ptr, &leaf.node)?;

        let mut ptr = leaf.ptr;
        while let Some(mut level) = stack.pop() {
            rt.realloc(&mut level.ptr);

            level.node.child[level.idx] = Some(ptr);
            if let Some((key, p)) = split {
                level.node.realloc_keys(rt.reborrow());
                split = level.node.insert(rt.reborrow(), p, level.idx, &key, true);
            }

            rt.io.write(level.ptr, &level.node)?;
            ptr = level.ptr;
        }

        if let Some((key, p)) = split {
            let parent_ptr = rt.alloc.alloc();

            let mut root = NodePage::empty();
            root.append_child(ptr);
            root.insert(rt.reborrow(), p, 0, &key, true);

            rt.io.write(parent_ptr, &root)?;
            ptr = parent_ptr;
        }

        Ok(ptr)
    }

    pub fn remove(
        self,
        mut rt: Rt<'_, impl Alloc, impl Free, impl AbstractIo>,
    ) -> io::Result<PagePtr<NodePage>> {
        let EntryInner {
            mut leaf,
            mut stack,
        } = self;

        rt.realloc(&mut leaf.ptr);
        let mut underflow = !leaf.node.can_donate();
        leaf.node.realloc_keys(rt.reborrow());
        let (_, _) = leaf
            .node
            .remove(rt.reborrow(), leaf.idx, false)
            .expect("just find");
        rt.io.write(leaf.ptr, &leaf.node)?;

        let mut prev = leaf.node;
        let mut ptr = leaf.ptr;

        let view = rt.io.read();
        while let Some(mut level) = stack.pop() {
            rt.realloc(&mut level.ptr);
            if underflow {
                level.node.realloc_keys(rt.reborrow());

                let mut left = (level.idx > 0).then(|| {
                    let ptr =
                        level.node.child[level.idx - 1].expect("left neighbor always present");
                    NodeWithPtr {
                        node: *view.page(ptr),
                        ptr,
                    }
                });
                let mut right = (level.idx < level.node.len() - 1)
                    .then(|| {
                        level.node.child[level.idx + 1].map(|ptr| NodeWithPtr {
                            node: *view.page(ptr),
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

                            rt.realloc(&mut donor.ptr);
                            donor.node.realloc_keys(rt.reborrow());
                            let (donated_ptr, donated_key) = donor
                                .node
                                .remove(rt.reborrow(), donor.node.len() - 1, true)
                                .expect("can donate");
                            prev.insert(rt.reborrow(), donated_ptr, 0, &donated_key, false);
                            rt.io.write(donor.ptr, &donor.node)?;
                            rt.io.write(ptr, &prev)?;

                            level.node.child[level.idx - 1] = Some(donor.ptr);

                            let parent_key =
                                donor.node.get_key(rt.reborrow(), donor.node.len() - 1);
                            level.node.set_key(rt.reborrow(), level.idx - 1, parent_key);

                            underflow = false;
                            break;
                        }
                    }

                    if let Some(donor) = &mut right {
                        if donor.can_donate() {
                            log::debug!("donate right");

                            rt.realloc(&mut donor.ptr);
                            donor.node.realloc_keys(rt.reborrow());
                            let (donated_ptr, donated_key) = donor
                                .node
                                .remove(rt.reborrow(), 0, false)
                                .expect("can donate");
                            prev.insert(rt.reborrow(), donated_ptr, K - 1, &donated_key, false);
                            rt.io.write(donor.ptr, &donor.node)?;
                            rt.io.write(ptr, &prev)?;

                            level.node.child[level.idx + 1] = Some(donor.ptr);

                            level.node.set_key(rt.reborrow(), level.idx, donated_key);

                            underflow = false;
                            break;
                        }
                    }

                    if let Some(neighbor) = &mut left {
                        if right.as_ref().map_or(true, |r| r.gt(neighbor)) {
                            log::debug!("merge left");
                            underflow = !level.node.can_donate();
                            rt.realloc(&mut neighbor.ptr);
                            neighbor.node.realloc_keys(rt.reborrow());
                            level.idx -= 1;
                            let (_, key) = level
                                .node
                                .remove(rt.reborrow(), level.idx, false)
                                .expect("must be there");
                            neighbor.node.merge(&prev, rt.reborrow(), key, false);
                            prev.free(rt.reborrow());
                            rt.free.free(ptr);
                            ptr = neighbor.ptr;
                            rt.io.write(neighbor.ptr, &neighbor.node)?;

                            break;
                        }
                    }

                    if let Some(neighbor) = right {
                        underflow = !level.node.can_donate();
                        log::debug!("merge right");
                        let (neighbor_ptr, _) = level
                            .node
                            .remove(rt.reborrow(), level.idx + 1, false)
                            .expect("must be there");
                        let key = level.node.get_key(rt.reborrow(), level.idx);
                        assert_eq!(neighbor_ptr, neighbor.ptr, "suppose to remove the neighbor");
                        let last_key = prev.merge(&neighbor.node, rt.reborrow(), key, true);
                        level.node.set_key(rt.reborrow(), level.idx, last_key);
                        neighbor.node.free(rt.reborrow());
                        rt.free.free(neighbor.ptr);
                        rt.io.write(ptr, &prev)?;

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
                level.node.child[level.idx] = Some(ptr);
                rt.io.write(level.ptr, &level.node)?;
                ptr = level.ptr;
                prev = level.node;
            }
        }

        Ok(ptr)
    }
}

struct NodeWithPtr {
    node: NodePage,
    ptr: PagePtr<NodePage>,
}

impl NodeWithPtr {
    fn can_donate(&self) -> bool {
        self.node.can_donate()
    }
}

impl PartialEq for NodeWithPtr {
    fn eq(&self, other: &Self) -> bool {
        self.node.len().eq(&other.node.len())
    }
}

impl PartialOrd for NodeWithPtr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.node.len().partial_cmp(&other.node.len())
    }
}

// for debug
#[cfg(test)]
pub fn print<K, D>(
    rt: Rt<'_, impl Alloc, impl Free, impl AbstractIo>,
    ptr: PagePtr<NodePage>,
    k: K,
    old: bool,
) where
    K: Fn(&[u8]) -> D,
    D: std::fmt::Display,
{
    // this is sad that I cannot debug B-Tree without using already existing B-Tree
    use std::collections::BTreeMap;

    let mut nodes = BTreeMap::new();
    let mut edges = Vec::new();

    fn print_inner<K, D>(
        mut rt: Rt<'_, impl Alloc, impl Free, impl AbstractIo>,
        ptr: PagePtr<NodePage>,
        nodes: &mut BTreeMap<u32, String>,
        edges: &mut Vec<(u32, u32)>,
        k: &K,
        old: bool,
    ) where
        K: Fn(&[u8]) -> D,
        D: std::fmt::Display,
    {
        let view = rt.io.read();
        let page = view.page(ptr);
        let node_text = (0..(page.len() - usize::from(!page.is_leaf())))
            .map(|idx| {
                if old {
                    page.get_key_old(&view, idx)
                } else {
                    page.get_key(rt.reborrow(), idx)
                }
            })
            .map(|key| format!("{}:{}", key.table_id, k(&key.bytes)))
            .collect::<Vec<_>>()
            .join("|");
        nodes.insert(ptr.raw_number(), format!("\"{node_text}\""));

        for n in (0..page.len()).map(|idx| page.child[idx].expect("BUG")) {
            edges.push((ptr.raw_number(), n.raw_number()));
            if !page.is_leaf() {
                print_inner(rt.reborrow(), n, nodes, edges, k, old);
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

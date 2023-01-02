use crate::concurrency;
use crate::concurrency::runtime::Runtime;
use crate::network_protocol::{Edge, EdgeState};
use crate::routing::bfs;
use crate::routing::routing_table_view::RoutingTableView;
use crate::stats::metrics;
use crate::store;
use crate::time;
use arc_swap::ArcSwap;
use near_primitives::network::PeerId;
use parking_lot::Mutex;
use rayon::iter::ParallelBridge;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[cfg(test)]
mod tests;

// TODO: make it opaque, so that the key.0 < key.1 invariant is protected.
type EdgeKey = (PeerId, PeerId);
pub type NextHopTable = HashMap<PeerId, Vec<PeerId>>;

#[derive(Clone)]
pub struct GraphConfig {
    pub node_id: PeerId,
    pub prune_unreachable_peers_after: time::Duration,
    pub prune_edges_after: Option<time::Duration>,
}

#[derive(Default)]
pub struct GraphSnapshot {
    pub edges: im::HashMap<EdgeKey, Edge>,
    pub local_edges: HashMap<PeerId, Edge>,
    pub next_hops: Arc<NextHopTable>,
}

struct Inner {
    config: GraphConfig,

    /// Current view of the network represented by an undirected graph.
    /// Contains only validated edges.
    /// Nodes are Peers and edges are active connections.
    graph: bfs::Graph,

    edges: im::HashMap<EdgeKey, Edge>,
    /// Last time a peer was reachable.
    peer_reachable_at: HashMap<PeerId, time::Instant>,
    store: store::Store,

    // Last time when we run the peers pruning.
    // This is quite expensive (as it touches DB) and we don't want to run it on every update.
    last_time_peers_pruned: Option<time::Instant>,
}

fn has(set: &im::HashMap<EdgeKey, Edge>, edge: &Edge) -> bool {
    set.get(&edge.key()).map_or(false, |x| x.nonce() >= edge.nonce())
}

impl Inner {
    /// Adds an edge without validating the signatures. O(1).
    /// Returns true, iff <edge> was newer than an already known version of this edge.
    fn update_edge(&mut self, now: time::Utc, edge: Edge) -> bool {
        if has(&self.edges, &edge) {
            return false;
        }
        if let Some(prune_edges_after) = self.config.prune_edges_after {
            // Don't add edges that are older than the limit.
            if edge.is_edge_older_than(now - prune_edges_after) {
                return false;
            }
        }
        let key = edge.key();
        // Add the edge.
        match edge.edge_type() {
            EdgeState::Active => self.graph.add_edge(&key.0, &key.1),
            EdgeState::Removed => self.graph.remove_edge(&key.0, &key.1),
        }
        self.edges.insert(key.clone(), edge);
        true
    }

    /// Removes an edge by key. O(1).
    fn remove_edge(&mut self, key: &EdgeKey) {
        if self.edges.remove(key).is_some() {
            self.graph.remove_edge(&key.0, &key.1);
        }
    }

    /// Removes all edges adjacent to the peers from the set.
    /// It is used to prune unreachable connected components from the inmem graph.
    fn remove_adjacent_edges(&mut self, peers: &HashSet<PeerId>) -> Vec<Edge> {
        let mut edges = vec![];
        for e in self.edges.clone().values() {
            if peers.contains(&e.key().0) || peers.contains(&e.key().1) {
                self.remove_edge(e.key());
                edges.push(e.clone());
            }
        }
        edges
    }

    fn prune_old_edges(&mut self, prune_edges_older_than: time::Utc) {
        for e in self.edges.clone().values() {
            if e.is_edge_older_than(prune_edges_older_than) {
                self.remove_edge(e.key());
            }
        }
    }

    /// If peer_id is not in memory check if it is on disk in bring it back on memory.
    ///
    /// Note: here an advanced example, which shows what's happening.
    /// Let's say we have a full graph fully connected with nodes `A, B, C, D`.
    /// Step 1 ) `A`, `B` get removed.
    /// We store edges belonging to `A` and `B`: `<A,B>, <A,C>, <A, D>, <B, C>, <B, D>`
    /// into component 1 let's call it `C_1`.
    /// And mapping from `A` to `C_1`, and from `B` to `C_1`
    ///
    /// Note that `C`, `D` is still active.
    ///
    /// Step 2) 'C' gets removed.
    /// We stored edges <C, D> into component 2 `C_2`.
    /// And a mapping from `C` to `C_2`.
    ///
    /// Note that `D` is still active.
    ///
    /// Step 3) An active edge gets added from `D` to `A`.
    /// We will load `C_1` and try to re-add all edges belonging to `C_1`.
    /// We will add `<A,B>, <A,C>, <A, D>, <B, C>, <B, D>`
    ///
    /// Important note: `C_1` also contains an edge from `A` to `C`, though `C` was removed in `C_2`.
    /// - 1) We will not load edges belonging to `C_2`, even though we are adding an edges from `A` to deleted `C`.
    /// - 2) We will not delete mapping from `C` to `C_2`, because `C` doesn't belong to `C_1`.
    /// - 3) Later, `C` will be deleted, because we will figure out it's not reachable.
    /// New component `C_3` will be created.
    /// And mapping from `C` to `C_2` will be overridden by mapping from `C` to `C_3`.
    /// And therefore `C_2` component will become unreachable.
    /// TODO(gprusak): this whole algorithm seems to be leaking stuff to storage and never cleaning up.
    /// What is the point of it? What does it actually gives us?
    fn load_component(&mut self, now: time::Utc, peer_id: PeerId) {
        if peer_id == self.config.node_id || self.peer_reachable_at.contains_key(&peer_id) {
            return;
        }
        let edges = match self.store.pop_component(&peer_id) {
            Ok(edges) => edges,
            Err(e) => {
                tracing::warn!("self.store.pop_component({}): {}", peer_id, e);
                return;
            }
        };
        for e in edges {
            self.update_edge(now, e);
        }
    }

    /// Prunes peers unreachable since <unreachable_since> (and their adjacent edges)
    /// from the in-mem graph and stores them in DB.
    fn prune_unreachable_peers(&mut self, unreachable_since: time::Instant) {
        // Select peers to prune.
        let mut peers = HashSet::new();
        for k in self.edges.keys() {
            for peer_id in [&k.0, &k.1] {
                if self
                    .peer_reachable_at
                    .get(peer_id)
                    .map(|t| t < &unreachable_since)
                    .unwrap_or(true)
                {
                    peers.insert(peer_id.clone());
                }
            }
        }
        if peers.is_empty() {
            return;
        }

        // Prune peers from peer_reachable_at.
        for peer_id in &peers {
            self.peer_reachable_at.remove(&peer_id);
        }

        // Prune edges from graph.
        let edges = self.remove_adjacent_edges(&peers);

        // Store the pruned data in DB.
        if let Err(e) = self.store.push_component(&peers, &edges) {
            tracing::warn!("self.store.push_component(): {}", e);
        }
    }

    /// 1. Adds edges to the graph (edges are expected to be already validated).
    /// 2. Prunes expired edges.
    /// 3. Prunes unreachable graph components.
    /// 4. Recomputes GraphSnapshot.
    /// Returns a subset of `edges`, consisting of edges which were not in the graph before.
    pub fn update(
        &mut self,
        clock: &time::Clock,
        mut edges: Vec<Edge>,
        unreliable_peers: &HashSet<PeerId>,
    ) -> (Vec<Edge>, GraphSnapshot) {
        let _update_time = metrics::ROUTING_TABLE_RECALCULATION_HISTOGRAM.start_timer();
        let total = edges.len();
        // load the components BEFORE updating the edges.
        // so that result doesn't contain edges we already have in storage.
        // It is especially important for initial full sync with peers, because
        // we broadcast all the returned edges to all connected peers.
        let now = clock.now_utc();
        for edge in &edges {
            let key = edge.key();
            self.load_component(now, key.0.clone());
            self.load_component(now, key.1.clone());
        }
        edges.retain(|e| self.update_edge(now, e.clone()));
        // Update metrics after edge update
        if let Some(prune_edges_after) = self.config.prune_edges_after {
            self.prune_old_edges(now - prune_edges_after);
        }
        let next_hops = Arc::new(self.graph.calculate_distance(unreliable_peers));

        // Update peer_reachable_at.
        let now = clock.now();
        self.peer_reachable_at.insert(self.config.node_id.clone(), now);
        for peer in next_hops.keys() {
            self.peer_reachable_at.insert(peer.clone(), now);
        }
        // Prune unreachable peers from time to time.
        // Peers are removed if they are not reacheable for prune_unreachable_peers_after - so we run the pruning every
        // prune_unreachable_peers_after / 2 seconds (so peer can be in the memory for max 1.5 * prune_unreachable_peers_after).
        if self
            .last_time_peers_pruned
            .map_or(true, |t| t < now - self.config.prune_unreachable_peers_after / 2)
        {
            self.prune_unreachable_peers(now - self.config.prune_unreachable_peers_after);
            self.last_time_peers_pruned = Some(now);
        }
        let mut local_edges = HashMap::new();
        for e in self.edges.clone().values() {
            if let Some(other) = e.other(&self.config.node_id) {
                local_edges.insert(other.clone(), e.clone());
            }
        }
        metrics::ROUTING_TABLE_RECALCULATIONS.inc();
        metrics::PEER_REACHABLE.set(next_hops.len() as i64);
        metrics::EDGE_UPDATES.inc_by(total as u64);
        metrics::EDGE_ACTIVE.set(self.graph.total_active_edges() as i64);
        metrics::EDGE_TOTAL.set(self.edges.len() as i64);
        (edges, GraphSnapshot { edges: self.edges.clone(), local_edges, next_hops })
    }
}

pub(crate) struct Graph {
    inner: Arc<Mutex<Inner>>,
    snapshot: ArcSwap<GraphSnapshot>,
    unreliable_peers: ArcSwap<HashSet<PeerId>>,
    // TODO(gprusak): RoutingTableView consists of a bunch of unrelated stateful features.
    // It requires a refactor.
    pub routing_table: RoutingTableView,

    runtime: Runtime,
}

impl Graph {
    pub fn new(config: GraphConfig, store: store::Store) -> Self {
        Self {
            routing_table: RoutingTableView::new(store.clone()),
            inner: Arc::new(Mutex::new(Inner {
                graph: bfs::Graph::new(config.node_id.clone()),
                config,
                edges: Default::default(),
                peer_reachable_at: HashMap::new(),
                store,
                last_time_peers_pruned: None,
            })),
            unreliable_peers: ArcSwap::default(),
            snapshot: ArcSwap::default(),
            runtime: Runtime::new(),
        }
    }

    pub fn load(&self) -> Arc<GraphSnapshot> {
        self.snapshot.load_full()
    }

    pub fn set_unreliable_peers(&self, unreliable_peers: HashSet<PeerId>) {
        self.unreliable_peers.store(Arc::new(unreliable_peers));
    }

    /// Verifies edge signatures on rayon runtime.
    /// Since this is expensive it first deduplicates the input edges
    /// and strips any edges which are already present in the graph.
    pub async fn verify(&self, edges: Vec<Edge>) -> (Vec<Edge>, bool) {
        let old = self.load();
        let mut edges = Edge::deduplicate(edges);
        edges.retain(|x| !has(&old.edges, x));
        // Verify the edges in parallel on rayon.
        concurrency::rayon::run(move || {
            concurrency::rayon::try_map(edges.into_iter().par_bridge(), |e| {
                if e.verify() {
                    Some(e)
                } else {
                    None
                }
            })
        })
        .await
    }

    /// Adds edges to the graph and recomputes the routing table.
    /// Returns the edges which were actually new and should be broadcasted.
    pub async fn update(self: &Arc<Self>, clock: &time::Clock, edges: Vec<Edge>) -> Vec<Edge> {
        // Computation is CPU heavy and accesses DB so we execute it on a dedicated thread.
        // TODO(gprusak): It would be better to move CPU heavy stuff to rayon and make DB calls async,
        // but that will require further refactor. Or even better: get rid of the Graph all
        // together.
        let this = self.clone();
        let clock = clock.clone();
        self.runtime
            .handle
            .spawn(async move {
                let mut inner = this.inner.lock();
                let (new_edges, snapshot) =
                    inner.update(&clock, edges, &this.unreliable_peers.load());
                let snapshot = Arc::new(snapshot);
                this.routing_table.update(snapshot.next_hops.clone());
                this.snapshot.store(snapshot);
                new_edges
            })
            .await
            .unwrap()
    }
}

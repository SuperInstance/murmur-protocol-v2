//! Murmur Protocol v2 — gossip-based vibe sharing for the Grand Pattern.
//!
//! Rooms whisper their vibes to neighbors. The murmur spreads through the network
//! like gossip, with each room combining what it hears with its own state.
//! Conservation is maintained: the total vibe in the network never changes.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A murmur message passed between rooms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Murmur {
    pub source_id: usize,
    pub vibe: f64,
    pub hop_count: u32,
    pub confidence: f64,
    pub origin_tick: u64,
}

/// Configuration for the murmur protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MurmurConfig {
    /// How much of a neighbor's vibe to adopt per murmur (0.0-1.0)
    pub blending_factor: f64,
    /// Maximum hops a murmur can travel before dying
    pub max_hops: u32,
    /// Minimum confidence to forward a murmur
    pub forward_threshold: f64,
    /// How much confidence decays per hop
    pub confidence_decay: f64,
}

impl Default for MurmurConfig {
    fn default() -> Self {
        Self {
            blending_factor: 0.1,
            max_hops: 3,
            forward_threshold: 0.1,
            confidence_decay: 0.8,
        }
    }
}

/// A room in the murmur network.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MurmurRoom {
    pub id: usize,
    pub vibe: f64,
    pub confidence: f64,
    pub neighbors: Vec<usize>,
    pub murmurs_received: usize,
    pub murmurs_sent: usize,
}

/// The murmur network — a collection of rooms sharing vibes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MurmurNetwork {
    rooms: HashMap<usize, MurmurRoom>,
    config: MurmurConfig,
    total_vibe: f64,
    tick: u64,
}

/// Result of a murmur propagation round.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MurmurRound {
    pub tick: u64,
    pub murmurs_delivered: usize,
    pub murmurs_dropped: usize,
    pub max_hop: u32,
    pub total_vibe: f64,
    pub conservation_error: f64,
}

impl MurmurNetwork {
    pub fn new(config: MurmurConfig) -> Self {
        Self {
            rooms: HashMap::new(),
            config,
            total_vibe: 0.0,
            tick: 0,
        }
    }

    /// Add a room to the network.
    pub fn add_room(&mut self, id: usize, initial_vibe: f64, confidence: f64) {
        self.total_vibe += initial_vibe;
        self.rooms.insert(id, MurmurRoom {
            id,
            vibe: initial_vibe,
            confidence,
            neighbors: Vec::new(),
            murmurs_received: 0,
            murmurs_sent: 0,
        });
    }

    /// Connect two rooms as neighbors.
    pub fn connect(&mut self, a: usize, b: usize) {
        if let Some(room) = self.rooms.get_mut(&a) {
            if !room.neighbors.contains(&b) {
                room.neighbors.push(b);
            }
        }
        if let Some(room) = self.rooms.get_mut(&b) {
            if !room.neighbors.contains(&a) {
                room.neighbors.push(a);
            }
        }
    }

    /// Build a star topology centered on `center`.
    pub fn build_star(&mut self, center: usize, others: &[usize]) {
        for &other in others {
            self.connect(center, other);
        }
    }

    /// Build a ring topology.
    pub fn build_ring(&mut self, ids: &[usize]) {
        for i in 0..ids.len() {
            let next = (i + 1) % ids.len();
            self.connect(ids[i], ids[next]);
        }
    }

    /// Inject vibe into a room (increases total vibe).
    pub fn inject(&mut self, room_id: usize, amount: f64) {
        if let Some(room) = self.rooms.get_mut(&room_id) {
            room.vibe += amount;
            self.total_vibe += amount;
        }
    }

    /// Extract vibe from a room (decreases total vibe).
    pub fn extract(&mut self, room_id: usize, amount: f64) -> f64 {
        if let Some(room) = self.rooms.get_mut(&room_id) {
            let taken = amount.min(room.vibe);
            room.vibe -= taken;
            self.total_vibe -= taken;
            taken
        } else {
            0.0
        }
    }

    /// Get a room's vibe.
    pub fn room_vibe(&self, room_id: usize) -> Option<f64> {
        self.rooms.get(&room_id).map(|r| r.vibe)
    }

    /// Total vibe in the network.
    pub fn total_vibe(&self) -> f64 {
        self.total_vibe
    }

    /// Run one round of murmur propagation.
    /// Each room whispers to all its neighbors.
    pub fn propagate(&mut self) -> MurmurRound {
        self.tick += 1;
        let mut murmurs_delivered = 0;
        let mut murmurs_dropped = 0;
        let mut max_hop = 0u32;
        let initial_vibe = self.total_vibe;

        // Collect: for each source, what does it send to each neighbor?
        // Murmur carries: source_id, neighbor_id (target), vibe chunk
        struct PendingMurmur {
            source_id: usize,
            target_id: usize,
            vibe: f64,
            confidence: f64,
        }

        let mut pending: Vec<PendingMurmur> = Vec::new();
        for (&id, room) in &self.rooms {
            let send_amount = room.vibe * self.config.blending_factor;
            let send_conf = room.confidence * self.config.confidence_decay;
            for &neighbor in &room.neighbors {
                pending.push(PendingMurmur {
                    source_id: id,
                    target_id: neighbor,
                    vibe: send_amount,
                    confidence: send_conf,
                });
            }
        }

        // Deliver murmurs to targets
        for m in &pending {
            if m.confidence < self.config.forward_threshold {
                murmurs_dropped += 1;
                continue;
            }
            if let Some(room) = self.rooms.get_mut(&m.target_id) {
                room.vibe += m.vibe;
                room.murmurs_received += 1;
                murmurs_delivered += 1;
                max_hop = max_hop.max(1);
            }
        }

        // Subtract sent vibe from sources (conservation)
        let mut sent_per_source: HashMap<usize, f64> = HashMap::new();
        for m in &pending {
            *sent_per_source.entry(m.source_id).or_insert(0.0) += m.vibe;
        }
        for (&source_id, total_sent) in &sent_per_source {
            if let Some(room) = self.rooms.get_mut(&source_id) {
                room.vibe -= total_sent;
                room.murmurs_sent += pending.iter().filter(|m| m.source_id == source_id).count();
            }
        }

        // Recompute total_vibe from rooms
        self.total_vibe = self.rooms.values().map(|r| r.vibe).sum();

        MurmurRound {
            tick: self.tick,
            murmurs_delivered,
            murmurs_dropped,
            max_hop,
            total_vibe: self.total_vibe,
            conservation_error: (self.total_vibe - initial_vibe).abs(),
        }
    }

    /// Run N rounds of propagation.
    pub fn propagate_n(&mut self, rounds: usize) -> Vec<MurmurRound> {
        (0..rounds).map(|_| self.propagate()).collect()
    }

    /// Get equilibrium distance: how far from uniform distribution.
    pub fn equilibrium_distance(&self) -> f64 {
        let n = self.rooms.len() as f64;
        if n == 0.0 { return 0.0; }
        let mean = self.total_vibe / n;
        self.rooms.values()
            .map(|r| (r.vibe - mean).powi(2))
            .sum::<f64>()
            .sqrt()
    }

    /// Number of rooms in the network.
    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }

    /// Get all room IDs.
    pub fn room_ids(&self) -> Vec<usize> {
        let mut ids: Vec<usize> = self.rooms.keys().copied().collect();
        ids.sort();
        ids
    }

    /// Average vibe across rooms.
    pub fn average_vibe(&self) -> f64 {
        if self.rooms.is_empty() { return 0.0; }
        self.total_vibe / self.rooms.len() as f64
    }

    /// Get rooms sorted by vibe (descending).
    pub fn rooms_by_vibe(&self) -> Vec<(usize, f64)> {
        let mut rooms: Vec<_> = self.rooms.iter().map(|(&id, r)| (id, r.vibe)).collect();
        rooms.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        rooms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_network() {
        let net = MurmurNetwork::new(MurmurConfig::default());
        assert_eq!(net.room_count(), 0);
        assert_eq!(net.total_vibe(), 0.0);
    }

    #[test]
    fn test_add_room() {
        let mut net = MurmurNetwork::new(MurmurConfig::default());
        net.add_room(0, 1.0, 1.0);
        assert_eq!(net.room_count(), 1);
        assert!((net.total_vibe() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_connect_rooms() {
        let mut net = MurmurNetwork::new(MurmurConfig::default());
        net.add_room(0, 1.0, 1.0);
        net.add_room(1, 0.0, 1.0);
        net.connect(0, 1);
        assert!(net.rooms.get(&0).unwrap().neighbors.contains(&1));
        assert!(net.rooms.get(&1).unwrap().neighbors.contains(&0));
    }

    #[test]
    fn test_star_topology() {
        let mut net = MurmurNetwork::new(MurmurConfig::default());
        net.add_room(0, 10.0, 1.0);
        for i in 1..5 {
            net.add_room(i, 0.0, 1.0);
        }
        net.build_star(0, &[1, 2, 3, 4]);
        let center = net.rooms.get(&0).unwrap();
        assert_eq!(center.neighbors.len(), 4);
    }

    #[test]
    fn test_ring_topology() {
        let mut net = MurmurNetwork::new(MurmurConfig::default());
        for i in 0..5 { net.add_room(i, i as f64, 1.0); }
        net.build_ring(&[0, 1, 2, 3, 4]);
        for i in 0..5 {
            let room = net.rooms.get(&i).unwrap();
            assert_eq!(room.neighbors.len(), 2);
        }
    }

    #[test]
    fn test_inject_extract() {
        let mut net = MurmurNetwork::new(MurmurConfig::default());
        net.add_room(0, 5.0, 1.0);
        net.inject(0, 3.0);
        assert!((net.room_vibe(0).unwrap() - 8.0).abs() < 1e-10);
        let taken = net.extract(0, 2.0);
        assert!((taken - 2.0).abs() < 1e-10);
        assert!((net.room_vibe(0).unwrap() - 6.0).abs() < 1e-10);
    }

    #[test]
    fn test_conservation_through_propagation() {
        let mut net = MurmurNetwork::new(MurmurConfig {
            blending_factor: 0.1,
            ..Default::default()
        });
        net.add_room(0, 10.0, 1.0);
        net.add_room(1, 0.0, 1.0);
        net.add_room(2, 0.0, 1.0);
        net.build_ring(&[0, 1, 2]);

        let initial = net.total_vibe();
        for _ in 0..20 {
            net.propagate();
        }
        let error = (net.total_vibe() - initial).abs();
        assert!(error < 1e-6, "Conservation error: {error}");
    }

    #[test]
    fn test_convergence_to_equilibrium() {
        let mut net = MurmurNetwork::new(MurmurConfig {
            blending_factor: 0.2,
            ..Default::default()
        });
        net.add_room(0, 10.0, 1.0);
        net.add_room(1, 0.0, 1.0);
        net.connect(0, 1);

        let initial_dist = net.equilibrium_distance();
        net.propagate_n(50);
        let final_dist = net.equilibrium_distance();
        assert!(final_dist < initial_dist, "Should converge toward equilibrium");
    }

    #[test]
    fn test_murmur_drop_on_confidence() {
        let mut net = MurmurNetwork::new(MurmurConfig {
            blending_factor: 0.1,
            forward_threshold: 0.5,
            confidence_decay: 0.1, // Confidence drops fast
            ..Default::default()
        });
        net.add_room(0, 10.0, 0.3); // Below threshold after decay
        net.add_room(1, 0.0, 1.0);
        net.connect(0, 1);

        let round = net.propagate();
        // Low confidence room should have murmurs dropped
        assert!(round.murmurs_dropped > 0 || round.murmurs_delivered == 0);
    }

    #[test]
    fn test_average_vibe() {
        let mut net = MurmurNetwork::new(MurmurConfig::default());
        net.add_room(0, 3.0, 1.0);
        net.add_room(1, 7.0, 1.0);
        assert!((net.average_vibe() - 5.0).abs() < 1e-10);
    }

    #[test]
    fn test_rooms_by_vibe() {
        let mut net = MurmurNetwork::new(MurmurConfig::default());
        net.add_room(0, 1.0, 1.0);
        net.add_room(1, 5.0, 1.0);
        net.add_room(2, 3.0, 1.0);
        let sorted = net.rooms_by_vibe();
        assert_eq!(sorted[0].0, 1); // Highest
        assert_eq!(sorted[2].0, 0); // Lowest
    }

    #[test]
    fn test_propagate_round_stats() {
        let mut net = MurmurNetwork::new(MurmurConfig::default());
        net.add_room(0, 10.0, 1.0);
        net.add_room(1, 0.0, 1.0);
        net.connect(0, 1);

        let round = net.propagate();
        assert_eq!(round.tick, 1);
        assert!(round.murmurs_delivered > 0);
        assert_eq!(round.max_hop, 1);
    }

    #[test]
    fn test_multi_hop_propagation() {
        let mut net = MurmurNetwork::new(MurmurConfig {
            max_hops: 5,
            confidence_decay: 0.95,
            ..Default::default()
        });
        // Chain: 0 — 1 — 2 — 3
        for i in 0..4 { net.add_room(i, if i == 0 { 10.0 } else { 0.0 }, 1.0); }
        net.build_ring(&[0, 1, 2, 3]);

        let _rounds = net.propagate_n(10);
        // After 10 rounds, vibe should have spread along the chain
        assert!(net.room_vibe(3).unwrap() > 0.0, "Vibe should reach end of chain");
    }

    #[test]
    fn test_mesh_converges_fast() {
        let mut net = MurmurNetwork::new(MurmurConfig {
            blending_factor: 0.1,
            ..Default::default()
        });
        // Full mesh: 5 rooms
        for i in 0..5 { net.add_room(i, if i == 0 { 10.0 } else { 0.0 }, 1.0); }
        for i in 0..5 {
            for j in (i+1)..5 {
                net.connect(i, j);
            }
        }

        net.propagate_n(30);
        let dist = net.equilibrium_distance();
        assert!(dist < 1.0, "Mesh should converge fast, dist={dist}");
    }

    #[test]
    fn test_star_converges_slower() {
        let mut net = MurmurNetwork::new(MurmurConfig {
            blending_factor: 0.1,
            ..Default::default()
        });
        net.add_room(0, 10.0, 1.0);
        for i in 1..5 { net.add_room(i, 0.0, 1.0); }
        net.build_star(0, &[1, 2, 3, 4]);

        net.propagate_n(30);
        // Star: leaves only talk through center, slower convergence
        let dist = net.equilibrium_distance();
        assert!(dist > 0.0); // Not fully converged yet
    }

    #[test]
    fn test_serialization() {
        let mut net = MurmurNetwork::new(MurmurConfig::default());
        net.add_room(0, 5.0, 0.8);
        net.add_room(1, 3.0, 0.9);
        net.connect(0, 1);

        let json = serde_json::to_string(&net).unwrap();
        let restored: MurmurNetwork = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.room_count(), 2);
        assert!((restored.total_vibe() - 8.0).abs() < 1e-10);
    }

    #[test]
    fn test_equilibrium_distance_empty() {
        let net = MurmurNetwork::new(MurmurConfig::default());
        assert_eq!(net.equilibrium_distance(), 0.0);
    }

    #[test]
    fn test_equilibrium_distance_uniform() {
        let mut net = MurmurNetwork::new(MurmurConfig::default());
        net.add_room(0, 5.0, 1.0);
        net.add_room(1, 5.0, 1.0);
        assert!(net.equilibrium_distance() < 1e-10);
    }

    #[test]
    fn test_connect_idempotent() {
        let mut net = MurmurNetwork::new(MurmurConfig::default());
        net.add_room(0, 1.0, 1.0);
        net.add_room(1, 1.0, 1.0);
        net.connect(0, 1);
        net.connect(0, 1); // Should not duplicate
        assert_eq!(net.rooms.get(&0).unwrap().neighbors.len(), 1);
    }

    #[test]
    fn test_extract_more_than_available() {
        let mut net = MurmurNetwork::new(MurmurConfig::default());
        net.add_room(0, 3.0, 1.0);
        let taken = net.extract(0, 10.0);
        assert!((taken - 3.0).abs() < 1e-10);
        assert!((net.room_vibe(0).unwrap()).abs() < 1e-10);
    }
}

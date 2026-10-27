use std::collections::{HashMap, HashSet, VecDeque};

use super::{
    scheduler::{PauseReason, Tracker},
    ConnectionEvents,
};

// Disconnected clean sessions have no resumable state; keep only recent metrics.
const MAX_METRICS_HISTORY: usize = 256;

pub struct Graveyard {
    connections: HashMap<String, SavedState>,
    metrics_order: VecDeque<String>,
}

impl Graveyard {
    pub fn new() -> Graveyard {
        Graveyard {
            connections: HashMap::new(),
            metrics_order: VecDeque::new(),
        }
    }

    /// Add a new connection.
    /// Return tracker of previous connection if connection id already exists
    pub fn retrieve(&mut self, id: &str) -> Option<SavedState> {
        self.metrics_order.retain(|entry| entry != id);
        self.connections.remove(id)
    }

    /// Save connection tracker
    pub fn save_state(
        &mut self,
        mut tracker: Tracker,
        subscriptions: HashSet<String>,
        metrics: ConnectionEvents,
        unacked_pubrels: VecDeque<u16>,
    ) {
        tracker.pause(PauseReason::Busy);
        let id = tracker.id.clone();
        self.metrics_order.retain(|entry| entry != &id);

        let session_state = SessionState {
            tracker,
            subscriptions,
            unacked_pubrels,
        };

        self.connections.insert(
            id,
            SavedState {
                session_state: Some(session_state),
                metrics,
            },
        );
    }

    /// Save only metrics for connection
    pub fn save_metrics(&mut self, id: String, metrics: ConnectionEvents) {
        self.metrics_order.retain(|entry| entry != &id);
        while self.metrics_order.len() >= MAX_METRICS_HISTORY {
            if let Some(oldest) = self.metrics_order.pop_front() {
                self.connections.remove(&oldest);
            }
        }
        self.metrics_order.push_back(id.clone());
        self.connections.insert(
            id,
            SavedState {
                session_state: None,
                metrics,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_session_churn_has_bounded_history() {
        let mut history = Graveyard::new();
        for i in 0..10_000 {
            history.save_metrics(format!("probe-{i}"), ConnectionEvents::default());
        }
        assert_eq!(history.connections.len(), MAX_METRICS_HISTORY);
        assert!(history.retrieve("probe-0").is_none());
        assert!(history.retrieve("probe-9999").is_some());
        assert_eq!(history.metrics_order.len(), MAX_METRICS_HISTORY - 1);
        for _ in 0..1000 {
            history.save_metrics("same-id".into(), ConnectionEvents::default());
        }
        assert_eq!(history.connections.len(), MAX_METRICS_HISTORY);
        assert_eq!(history.metrics_order.len(), MAX_METRICS_HISTORY);
    }

    #[test]
    fn metrics_eviction_preserves_persistent_sessions() {
        let mut history = Graveyard::new();
        // Replacing metrics with resumable state must also remove its eviction entry.
        history.save_metrics("persistent".into(), ConnectionEvents::default());
        history.save_state(
            Tracker::new("persistent".into()),
            HashSet::from(["devices/#".into()]),
            ConnectionEvents::default(),
            VecDeque::from([42]),
        );
        for i in 0..10_000 {
            history.save_metrics(format!("probe-{i}"), ConnectionEvents::default());
        }
        assert_eq!(history.connections.len(), MAX_METRICS_HISTORY + 1);
        let state = history
            .retrieve("persistent")
            .unwrap()
            .session_state
            .unwrap();
        assert!(state.subscriptions.contains("devices/#"));
        assert_eq!(state.unacked_pubrels, VecDeque::from([42]));
    }
}

#[derive(Debug)]
pub struct SavedState {
    pub session_state: Option<SessionState>,
    pub metrics: ConnectionEvents,
}

#[derive(Debug)]
pub struct SessionState {
    pub tracker: Tracker,
    pub subscriptions: HashSet<String>,
    // used for pubrel in qos2
    pub unacked_pubrels: VecDeque<u16>,
}

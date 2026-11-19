//! Queue protection must distinguish unused capacity from capacity exhausted by video.
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const WINDOW: Duration = Duration::from_secs(2);
const CUT_SPACING: Duration = Duration::from_millis(500);

#[derive(Default)]
pub(crate) struct VideoBrake {
    pub cap: Option<f64>,
    pub application_limited: bool,
    pub reason: &'static str,
    sent: VecDeque<(Instant, f64)>,
    high_rounds: u32,
    last_cut: Option<Instant>,
}

impl VideoBrake {
    /// `delivery` is measured over receiver arrival time, not bursts of feedback callbacks.
    /// Its least queue delay establishes whether the bottleneck stayed busy throughout the sample.
    pub fn observe(
        &mut self,
        now: Instant,
        budget: f64,
        allowance: f64,
        sent: f64,
        delivery: Option<(f64, f64)>,
        target_ms: f64,
    ) -> Option<f64> {
        self.sent.push_back((now, sent));
        while self.sent.front().is_some_and(|(at, _)| now.duration_since(*at) > WINDOW) {
            self.sent.pop_front();
        }
        let offered = self.sent.iter().map(|(_, rate)| *rate).fold(0.0, f64::max);
        // A quiet image consumes little of its allowance and its offered bytes still get through.
        // Low delivery alone is NOT evidence of this: on a constrained path offered > delivered.
        self.application_limited = delivery.is_some_and(|(rate, _)| offered > 0.0 && offered < allowance * 0.75 && rate >= offered * 0.8);
        let queue = delivery.map_or(0.0, |(_, least_queue)| least_queue);
        let congested = queue > target_ms / 2.0 && !self.application_limited;
        self.high_rounds = if congested { self.high_rounds.saturating_add(1) } else { 0 };
        if self.high_rounds >= 2 {
            self.reason = "persistent_queue";
            if self.last_cut.is_none_or(|at| now.duration_since(at) >= CUT_SPACING)
                && let Some((rate, _)) = delivery.filter(|(rate, _)| *rate > 0.0)
            {
                // Back off the measured delivery once. Do not multiply the previous cut by 0.85
                // again when the measured capacity has not changed.
                self.cap = Some(self.cap.unwrap_or(budget).min(rate * 0.85));
                self.last_cut = Some(now);
            }
        } else if !congested {
            self.reason = if delivery.is_none() {
                "no_fresh_feedback"
            } else if self.application_limited {
                "application_limited"
            } else {
                "queue_cleared"
            };
            // Grow below the same congestion threshold; no 75–150 ms recovery dead zone.
            self.cap = self.cap.and_then(|cap| (cap * 1.25 < budget).then_some(cap * 1.25));
        } else {
            self.reason = "confirming_queue";
        }
        self.cap
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quiet_video_does_not_infer_capacity_from_low_demand() {
        let start = Instant::now();
        let mut brake = VideoBrake::default();
        for tick in 0..120 {
            let cap = brake.observe(
                start + Duration::from_millis(tick * 250),
                64_000.0,
                64_000.0,
                2_000.0,
                Some((2_000.0, if tick % 8 < 3 { 200.0 } else { 100.0 })),
                300.0,
            );
            assert_eq!(cap, None);
        }
        assert!(brake.application_limited);
    }
    #[test]
    fn actual_capacity_shortfall_still_cuts_video() {
        let start = Instant::now();
        let mut brake = VideoBrake::default();
        for tick in 0..8 {
            brake.observe(start + Duration::from_millis(tick * 250), 100_000.0, 100_000.0, 90_000.0, Some((20_000.0, 250.0)), 300.0);
        }
        assert_eq!(brake.cap, Some(17_000.0));
        assert!(!brake.application_limited);
    }
    #[test]
    fn low_demand_with_real_delivery_shortfall_is_not_exempted() {
        let start = Instant::now();
        let mut brake = VideoBrake::default();
        for tick in 0..4 {
            brake.observe(start + Duration::from_millis(tick * 250), 64_000.0, 64_000.0, 2_000.0, Some((500.0, 200.0)), 300.0);
        }
        assert_eq!(brake.cap, Some(425.0));
        assert!(!brake.application_limited);
    }
    #[test]
    fn recovery_does_not_stall_in_the_old_dead_zone() {
        let start = Instant::now();
        let mut brake = VideoBrake::default();
        for tick in 0..2 {
            brake.observe(start + Duration::from_millis(tick * 250), 64_000.0, 64_000.0, 60_000.0, Some((50_000.0, 200.0)), 300.0);
        }
        assert_eq!(brake.cap, Some(42_500.0));
        for tick in 2..10 {
            brake.observe(start + Duration::from_millis(tick * 250), 64_000.0, 64_000.0, 50_000.0, Some((50_000.0, 100.0)), 300.0);
        }
        assert_eq!(brake.cap, None);
    }
    #[test]
    fn feedback_loss_releases_only_this_brake_and_bounds_history() {
        let start = Instant::now();
        let mut brake = VideoBrake { cap: Some(10_000.0), ..Default::default() };
        for tick in 0..120 {
            brake.observe(start + Duration::from_millis(tick * 250), 64_000.0, 64_000.0, 0.0, None, 300.0);
        }
        assert_eq!(brake.cap, None);
        assert!(brake.sent.len() <= 9);
        assert_eq!(brake.reason, "no_fresh_feedback");
    }
}

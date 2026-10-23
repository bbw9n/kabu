use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    delays: Vec<Duration>,
}

impl RetryPolicy {
    pub fn crypto_stream() -> Self {
        Self::from_seconds(&[1, 2, 4, 8, 16, 32, 60])
    }

    pub fn stock_poll() -> Self {
        Self::from_seconds(&[15, 30, 60, 120])
    }

    pub fn base_delay(&self, attempt: usize) -> Duration {
        self.delays[attempt.min(self.delays.len() - 1)]
    }

    fn from_seconds(seconds: &[u64]) -> Self {
        Self {
            delays: seconds.iter().copied().map(Duration::from_secs).collect(),
        }
    }
}

pub fn jittered_delay(base: Duration, sample: f64) -> Duration {
    let sample = sample.clamp(0.0, 1.0);
    base.mul_f64(0.8 + (sample * 0.4))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_retry_schedule_is_capped() {
        let policy = RetryPolicy::crypto_stream();
        let seconds: Vec<u64> = (0..8)
            .map(|attempt| policy.base_delay(attempt).as_secs())
            .collect();
        assert_eq!(seconds, vec![1, 2, 4, 8, 16, 32, 60, 60]);
    }

    #[test]
    fn stock_poll_retry_schedule_is_capped() {
        let policy = RetryPolicy::stock_poll();
        let seconds: Vec<u64> = (0..5)
            .map(|attempt| policy.base_delay(attempt).as_secs())
            .collect();
        assert_eq!(seconds, vec![15, 30, 60, 120, 120]);
    }

    #[test]
    fn jitter_is_clamped_to_a_deterministic_twenty_percent_range() {
        let base = std::time::Duration::from_secs(100);
        assert_eq!(
            jittered_delay(base, -1.0),
            std::time::Duration::from_secs(80)
        );
        assert_eq!(
            jittered_delay(base, 0.5),
            std::time::Duration::from_secs(100)
        );
        assert_eq!(
            jittered_delay(base, 2.0),
            std::time::Duration::from_secs(120)
        );
    }
}

use std::time::{SystemTime, UNIX_EPOCH};

pub struct Utils;

impl Utils {
    pub fn get_current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
    }

    pub fn get_current_timestamp_ms() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis()
    }

    pub fn format_number(num: f64, decimal_places: usize) -> String {
        format!("{:.1$}", num, decimal_places)
    }

    pub fn to_uppercase(s: &str) -> String {
        s.to_uppercase()
    }

    // Add more utility functions as needed
}
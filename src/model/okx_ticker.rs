use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct okx_ticker {
    pub ask_px: f64,
    pub ask_sz: f64,
    pub bid_px: f64,
    pub bid_sz: f64,
    pub high24h: f64,
    pub inst_id: String,
    pub inst_type: String,
    pub last: f64,
    pub last_sz: f64,
    pub low24h: f64,
    pub open24h: f64,
    pub sod_utc0: f64,
    pub sod_utc8: f64,
    pub ts: i64,
    pub vol24h: f64,
    pub vol_ccy24h: f64,
}

impl okx_ticker {
    pub fn new(
        ask_px: f64,
        ask_sz: f64,
        bid_px: f64,
        bid_sz: f64,
        high24h: f64,
        inst_id: String,
        inst_type: String,
        last: f64,
        last_sz: f64,
        low24h: f64,
        open24h: f64,
        sod_utc0: f64,
        sod_utc8: f64,
        ts: i64,
        vol24h: f64,
        vol_ccy24h: f64,
    ) -> Self {
        okx_ticker {
            ask_px,
            ask_sz,
            bid_px,
            bid_sz,
            high24h,
            inst_id,
            inst_type,
            last,
            last_sz,
            low24h,
            open24h,
            sod_utc0,
            sod_utc8,
            ts,
            vol24h,
            vol_ccy24h,
        }
    }
}
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderData {
    #[serde(rename = "instType")]
    pub inst_type: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "tgtCcy")]
    pub tgt_ccy: String,
    pub ccy: String,
    #[serde(rename = "ordId")]
    pub ord_id: String,
    #[serde(rename = "clOrdId")]
    pub cl_ord_id: String,
    #[serde(rename = "algoClOrdId")]
    pub algo_cl_ord_id: String,
    #[serde(rename = "algoId")]
    pub algo_id: String,
    pub tag: String,
    pub px: String,
    pub sz: String,
    #[serde(rename = "notionalUsd")]
    pub notional_usd: String,
    #[serde(rename = "ordType")]
    pub ord_type: String,
    pub side: String,
    #[serde(rename = "posSide")]
    pub pos_side: String,
    #[serde(rename = "tdMode")]
    pub td_mode: String,
    #[serde(rename = "accFillSz")]
    pub acc_fill_sz: String,
    #[serde(rename = "fillNotionalUsd")]
    pub fill_notional_usd: String,
    #[serde(rename = "avgPx")]
    pub avg_px: String,
    pub state: String,
    pub lever: String,
    pub pnl: String,
    #[serde(rename = "feeCcy")]
    pub fee_ccy: String,
    pub fee: String,
    #[serde(rename = "rebateCcy")]
    pub rebate_ccy: String,
    pub rebate: String,
    pub category: String,
    #[serde(rename = "uTime")]
    pub u_time: String,
    #[serde(rename = "cTime")]
    pub c_time: String,
    pub source: String,
    #[serde(rename = "reduceOnly")]
    pub reduce_only: String,
    #[serde(rename = "cancelSource")]
    pub cancel_source: String,
    #[serde(rename = "quickMgnType")]
    pub quick_mgn_type: String,
    #[serde(rename = "stpId")]
    pub stp_id: String,
    #[serde(rename = "stpMode")]
    pub stp_mode: String,
    #[serde(rename = "attachAlgoClOrdId")]
    pub attach_algo_cl_ord_id: String,
    #[serde(rename = "lastPx")]
    pub last_px: String,
    #[serde(rename = "isTpLimit")]
    pub is_tp_limit: String,
    #[serde(rename = "slTriggerPx")]
    pub sl_trigger_px: String,
    #[serde(rename = "slTriggerPxType")]
    pub sl_trigger_px_type: String,
    #[serde(rename = "tpOrdPx")]
    pub tp_ord_px: String,
    #[serde(rename = "tpTriggerPx")]
    pub tp_trigger_px: String,
    #[serde(rename = "tpTriggerPxType")]
    pub tp_trigger_px_type: String,
    #[serde(rename = "slOrdPx")]
    pub sl_ord_px: String,
    #[serde(rename = "fillPx")]
    pub fill_px: String,
    #[serde(rename = "tradeId")]
    pub trade_id: String,
    #[serde(rename = "fillSz")]
    pub fill_sz: String,
    #[serde(rename = "fillTime")]
    pub fill_time: String,
    #[serde(rename = "fillPnl")]
    pub fill_pnl: String,
    #[serde(rename = "fillFee")]
    pub fill_fee: String,
    #[serde(rename = "fillFeeCcy")]
    pub fill_fee_ccy: String,
    #[serde(rename = "execType")]
    pub exec_type: String,
    #[serde(rename = "fillPxVol")]
    pub fill_px_vol: String,
    #[serde(rename = "fillPxUsd")]
    pub fill_px_usd: String,
    #[serde(rename = "fillMarkVol")]
    pub fill_mark_vol: String,
    #[serde(rename = "fillFwdPx")]
    pub fill_fwd_px: String,
    #[serde(rename = "fillMarkPx")]
    pub fill_mark_px: String,
    #[serde(rename = "amendSource")]
    pub amend_source: String,
    #[serde(rename = "reqId")]
    pub req_id: String,
    #[serde(rename = "amendResult")]
    pub amend_result: String,
    pub code: String,
    pub msg: String,
    #[serde(rename = "pxType")]
    pub px_type: String,
    #[serde(rename = "pxUsd")]
    pub px_usd: String,
    #[serde(rename = "pxVol")]
    pub px_vol: String,
    #[serde(rename = "linkedAlgoOrd")]
    pub linked_algo_ord: LinkedAlgoOrd,
    #[serde(rename = "attachAlgoOrds")]
    pub attach_algo_ords: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkedAlgoOrd {
    #[serde(rename = "algoId")]
    pub algo_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Arg {
    pub channel: String,
    #[serde(rename = "instType")]
    pub inst_type: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
    pub uid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OkxOrderMessage {
    pub arg: Arg,
    pub data: Vec<OrderData>,
}

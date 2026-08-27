//! Top-of-Book & Trades feed wire format.

#![forbid(unsafe_code)]

pub mod quote;
pub mod trade;

pub use quote::{Quote, QUOTE_ASK_GONE, QUOTE_ASK_UPDATED, QUOTE_BID_GONE, QUOTE_BID_UPDATED};
pub use trade::{
    Trade, AGGRESSOR_BUY, AGGRESSOR_SELL, AGGRESSOR_UNKNOWN, TRADE_FLAG_BLOCK, TRADE_FLAG_CROSS,
    TRADE_FLAG_SWEEP,
};

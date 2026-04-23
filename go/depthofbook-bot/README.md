# DZ Depth-of-Book Bot

Reference Go subscriber that consumes the DoubleZero Depth-of-Book parser's Unix socket, maintains in-memory MBO order books per instrument, and persists per-event rows + coalesced top-N level snapshots + raw wire snapshots into ClickHouse.

Sibling to [topofbook-bot](../topofbook-bot/). Documentation will land as the implementation completes.

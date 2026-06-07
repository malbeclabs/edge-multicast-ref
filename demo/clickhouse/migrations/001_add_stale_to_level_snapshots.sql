ALTER TABLE marketbyorder.level_snapshots ADD COLUMN IF NOT EXISTS stale UInt8 DEFAULT 0;

CREATE UNIQUE INDEX IF NOT EXISTS uq_events_event_id ON events (event_id);
DROP INDEX IF EXISTS idx_events_event_id;

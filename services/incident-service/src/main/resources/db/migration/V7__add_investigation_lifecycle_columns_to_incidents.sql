-- Milestone M - an incident tracks how many times its evidence has changed
-- (signal_version), which run is currently authoritative, and whether a
-- follow-up investigation is owed once the active run finishes.

ALTER TABLE incidents ADD COLUMN signal_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE incidents ADD COLUMN current_investigation_run_id UUID;
ALTER TABLE incidents ADD COLUMN needs_reinvestigation BOOLEAN NOT NULL DEFAULT FALSE;

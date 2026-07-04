-- 2026-07-03_alerts_ack_note.sql
-- Add the missing ack_note column to cip.alerts.
--
-- schema.sql has long declared `ack_note STRING` on the alerts table, but the
-- live table predates that line and `CREATE TABLE IF NOT EXISTS` never adds a
-- column to an existing table, so the column was missing in the shared
-- staging+prod dataset. routers/alerts.py SELECTs a.ack_note, so GET
-- /api/alerts/ returned 500 and the frontend swallowed it as a false
-- "All clear", hiding the open alert backlog.
--
-- APPLIED 2026-07-03 to point-blank-ada.cip.alerts (region
-- northamerica-northeast1) out of band. This file records the change so the
-- dataset can be rebuilt from migrations. Idempotent: safe to re-run.

ALTER TABLE `point-blank-ada.cip.alerts`
ADD COLUMN IF NOT EXISTS ack_note STRING
OPTIONS(description="free-text action note recorded at acknowledgement");

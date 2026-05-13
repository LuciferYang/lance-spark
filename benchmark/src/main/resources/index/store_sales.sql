-- ZONEMAP indexes on store_sales fact table — exercises the distributed multi-fragment
-- build path added in c9dcf64 (one IndexMetadata segment per fragment, sharing the index
-- name). At sf=10 store_sales has roughly 28.8M rows distributed across ~29 fragments
-- (default max_rows_per_file=1M), so each `CREATE INDEX` here commits ~29 segments.
--
-- ss_sold_date_sk is the most-pruning join key for TPC-DS — many queries filter via
-- date_dim → date_sk fact-side join — so the zonemap on this column is what DFP would
-- consult at scan-plan time. The baseline we measure here (wall-clock + on-disk bytes)
-- will be the comparison point for the build-time-consolidated single-segment path.
ALTER TABLE store_sales CREATE INDEX idx_zm_ss_sold_date_sk USING zonemap (ss_sold_date_sk);

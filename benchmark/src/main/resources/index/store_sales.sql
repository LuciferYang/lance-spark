-- Fact-side join-key indexes on store_sales. These are the columns that TPC-DS
-- star-schema queries join against dimension tables; a ZONEMAP-backed btree on
-- each lets DFP inject runtime filters and prune fragments when a selective
-- dimension predicate restricts the join value set.
--
-- DFP efficacy also requires fact-side data clustering on the join key so each
-- fragment's zone bounds are tight — see DfpClusterRebuilder for the re-sort step.
ALTER TABLE store_sales CREATE INDEX idx_ss_sold_date_sk USING btree (ss_sold_date_sk);
ALTER TABLE store_sales CREATE INDEX idx_ss_item_sk USING btree (ss_item_sk);
ALTER TABLE store_sales CREATE INDEX idx_ss_customer_sk USING btree (ss_customer_sk);
ALTER TABLE store_sales CREATE INDEX idx_ss_store_sk USING btree (ss_store_sk);
ALTER TABLE store_sales CREATE INDEX idx_ss_promo_sk USING btree (ss_promo_sk);

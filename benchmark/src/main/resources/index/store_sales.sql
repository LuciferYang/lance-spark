ALTER TABLE store_sales CREATE INDEX idx_ss_sold_date_sk USING btree (ss_sold_date_sk);
ALTER TABLE store_sales CREATE INDEX idx_ss_item_sk USING btree (ss_item_sk);
ALTER TABLE store_sales CREATE INDEX idx_ss_customer_sk USING btree (ss_customer_sk);
ALTER TABLE store_sales CREATE INDEX idx_ss_store_sk USING btree (ss_store_sk);
ALTER TABLE store_sales CREATE INDEX idx_ss_promo_sk USING btree (ss_promo_sk);

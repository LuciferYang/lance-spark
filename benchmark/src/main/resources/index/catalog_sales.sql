ALTER TABLE catalog_sales CREATE INDEX idx_cs_sold_date_sk USING zonemap (cs_sold_date_sk);
ALTER TABLE catalog_sales CREATE INDEX idx_cs_item_sk USING zonemap (cs_item_sk);
ALTER TABLE catalog_sales CREATE INDEX idx_cs_bill_customer_sk USING zonemap (cs_bill_customer_sk);

ALTER TABLE web_sales CREATE INDEX idx_ws_sold_date_sk USING zonemap (ws_sold_date_sk);
ALTER TABLE web_sales CREATE INDEX idx_ws_item_sk USING zonemap (ws_item_sk);
ALTER TABLE web_sales CREATE INDEX idx_ws_bill_customer_sk USING zonemap (ws_bill_customer_sk);

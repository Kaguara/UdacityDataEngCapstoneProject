staging_transactions_table_drop_sql = "DROP TABLE IF EXISTS staging_transactions"
merchants_table_drop_sql = "DROP TABLE IF EXISTS merchants"
customers_table_drop_sql = "DROP TABLE IF EXISTS customers"

CREATE_STAGING_TRANSACTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging_transactions (
staging_transactions_key INTEGER IDENTITY(0,1),
step                    INTEGER,            
transaction_type         VARCHAR(MAX) distkey,
transaction_amount      DOUBLE PRECISION,
originator_name         VARCHAR(MAX),
originator_old_balance  DOUBLE PRECISION,
originator_new_balance  DOUBLE PRECISION,
recepient_name          VARCHAR(MAX),
recepient_old_balance   DOUBLE PRECISION,
recepient_new_balance   DOUBLE PRECISION,
isFraud                 INTEGER,
isFlaggedFraud          INTEGER
);
"""

CREATE_MERCHANTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS merchants (
merchant_key                 INTEGER IDENTITY(0,1),
merchant_name                VARCHAR(MAX),
cash_in_count                INTEGER,
cash_out_count               INTEGER,
payment_count                INTEGER,
distinct_customers_count     INTEGER
);
"""

CREATE_CUSTOMERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS customers (
customer_key                               INTEGER IDENTITY(0,1),
customer_name                              VARCHAR(MAX),
cash_in_count                              INTEGER,
cash_out_count                             INTEGER,
payment_count                              INTEGER,
transfer_count                             INTEGER,
first_transaction_step_time                INTEGER,
last_transaction_step_time                 INTEGER,
distinct_top_transfer_to_customer_count    INTEGER,
distinct_merchant_count				       INTEGER
);
"""

INSERT_INTO_MERCHANTS_TABLE_SQL = """
INSERT INTO merchants (merchant_name, cash_in_count, cash_out_count, payment_count, top_customer) 
SELECT distinct st.recepient_name, 
	sum(case when st.transaction_type = 'CASH-IN' then st.transaction_amount else 0 end),
	sum(case when st.transaction_type = 'CASH-OUT' then st.transaction_amount else 0 end), 
	sum(case when st.transaction_type = 'PAYMENT' then st.transaction_amount else 0 end),
	count(distinct st.originator_name)
FROM staging_transactions st
WHERE st.recepient_name like 'M%'
GROUP BY st.recepient_name, st.originator_name;
"""

INSERT_INTO_CUSTOMERS_TABLE_SQL = """
INSERT INTO customers (customer_name, cash_in_count, cash_out_count, payment_count, transfer_count, first_transaction_step_time , last_transaction_step_time, distinct_top_transfer_to_customer_count, distinct_merchant_count) 
SELECT distinct st.recepient_name, 
	sum(case when st.transaction_type = 'CASH-IN' then st.transaction_amount else 0 end),
	sum(case when st.transaction_type = 'CASH-OUT' then st.transaction_amount else 0 end), 
	sum(case when st.transaction_type = 'PAYMENT' then st.transaction_amount else 0 end),
	sum(case when st.transaction_type = 'TRANSFER' then st.transaction_amount else 0 end),
	min(st.step) as min_step_time,
	max(st.step) as max_step_time,
	count(case when st.transaction_type = 'TRANSFER' then st.recepient_name else '' end),
	count(case when st.recepient_name like 'M%' then st.recepient_name else '' end)
FROM staging_transactions st
GROUP BY st.recepient_name, st.originator_name;
"""
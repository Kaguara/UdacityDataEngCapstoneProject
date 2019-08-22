staging_transactions_table_drop_sql = "DROP TABLE IF EXISTS staging_transactions"

CREATE_STAGING_TRANSACTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging_transactions (
staging_transactions_key INTEGER IDENTITY(0,1),
step                    INTEGER,            
tansaction_type         VARCHAR(MAX) distkey,
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
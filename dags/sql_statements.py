staging_transactions_table_drop_sql = "DROP TABLE IF EXISTS staging_transactions"
merchants_table_drop_sql = "DROP TABLE IF EXISTS merchants"
customers_table_drop_sql = "DROP TABLE IF EXISTS customers"
world_bank_stats_table_drop_sql = "DROP TABLE IF EXISTS world_bank_stats"

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

CREATE_WORLD_BANK_STATS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS world_bank_stats (
stat_key                 INTEGER IDENTITY(0,1),
year
country_code
country_name
region
economic_status
Account_age_15_plus	
Account_male_age_15_plus	
Account_in_labor_force_age_15_plus 	
Account_out_of_labor_force_age_15_plus 	
Account_female_age_15_plus	
Account_young_adults_ages_15_24	
Account_older_adults_ages_25_plus	
Account_primary_education_or_less_ages_15_plus 	
Account_secondary_education_or_more_ages_15_plus 	
Account_income_poorest_40_percent_ages_15_plus	
Account_income_richest_60_ages_15_plus 	
Account_rural_age_15_plus 	
Financial_institution_account_age_15_plus 	
Financial_institution_account_male_age_15_plus 	
Financial_institution_account_in_labor_force_age_15_plus 	
Financial_institution_account_out_of_labor_force_age_15_plus 	
Financial_institution_account_female_age_15_plus 	
Financial_institution_account_young_adults_age_15_24 	
Financial_institution_account_older_adults_age_25_plus 	
Financial institution account_primary_education_or_less_age_15_plus 	
Financial_institution_account_seconday_education_or_more_age_15_plus 	
Financial_institution_account_income_poorest_40_percent_age_15_plus 	
Financial_institution_account_income_richest_60_age_15_plus 	
Financial_institution_account_rural_age_15_plus 	
Withdrawal_in_the_past_year_with_a_financial_institution_account_age_15_plus	
No_account_because_financial_institutions_are_too_far_away_age_15_plus 	
No_account_because_financial_institutions_are_too_far_away_without_a_financial_institution_account_age_15_plus 	
No_account_because_financial_services_are_too_expensive_age_15_plus 	
No_account_because_financial_services_are_too_expensive_without_a_financial_institution_account_age_15_plus 	
No_account_because_of_lack_of_necessary_documentation_age_15_plus 	
No_account_because_of_lack_of_necessary_documentation_without_a_financial_institution_account_age_15_plus 	
No_account_because_of_lack_of_trust_in_financial_institutions_age_15_plus 	
No_account_because_of_lack_of_trust_in_financial_institutions_without_a_financial_institution_account_age_15_plus 	
No_account_because_of_religious_reasons_age_15_plus 	
No_account_because_of_religious_reasons_without_a_financial_institution_account_age_15_plus 	
No_account_because_of_insufficient_funds_age_15_plus 	
No_account_because_of_insufficient_funds_without_a_financial_institution_account_age_15_plus 	
No_account_because_someone_in_the_family_has_an_account_age_15_plus 	
No_account_because_someone_in_the_family_has_an_account_without_a_financial_institution_account_age_15_plus 	
No_account_because_of_no_need_for_financial_services_ONLY_age_15_plus 	
No_account_because_of_no_need_for_financial_services_ONLY_without_a_financial_institution_account_age_15_plus 	
Main_mode_of_withdrawal_ATM_percentage_with_a_financial_institution_account_age_15_plus 	
Main_mode_of_withdrawal_bank_teller_percentage_with_a_financial_institution_account_age_15_plus 	
Used_the_internet_to_pay_bills_in_the_past_year_age_15_plus	
Used_the_internet_to_pay_bills_in_the_past_year_male_age_15_plus	
Used_the_internet_to_pay_bills_in_the_past_year_in_labor_force_age_15_plus	
Used_the_internet_to_pay_bills_in_the_past_year_out_of_labor_force_age_15_plus	
Used_the_internet_to_pay_bills_in_the_past_year_female_age_15_plus	
Used_the_internet_to_pay_bills_in_the_past_year_young_adults_age_15_24	
Used_the_internet_to_pay_bills_in_the_past_year_older_adults (% age 25+)	Used the internet to pay bills in the past year, primary education or less (% age 15+)	Used the internet to pay bills in the past year , secondary education or more (% age 15+)	Used the internet to pay bills in the past year, income, poorest 40%(% age 15+)	Used the internet to pay bills in the past year , income, richest 60%(% age 15+)	Used the internet to pay bills in the past year , rural (% age 15+)	Used the internet to pay bills or to buy something online in the past year (% age 15+)	Used the internet to pay bills or to buy something online in the past year, male (% age 15+)	Used the internet to pay bills or to buy something online in the past year, in labor force (% age 15+)	Used the internet to pay bills or to buy something online in the past year, out of labor force (% age 15+)	Used the internet to pay bills or to buy something online in the past year, female (% age 15+)	Used the internet to pay bills or to buy something online in the past year, young adults (% age 15-24)	Used the internet to pay bills or to buy something online in the past year, older adults (% age 25+)	Used the internet to pay bills or to buy something online in the past year, primary education or less (% age 15+)	Used the internet to pay bills or to buy something online in the past year, secondary education or less (% age 15+)	Used the internet to pay bills or to buy something online in the past year, income, poorest 40% (% age 15+)	Used the internet to pay bills or to buy something online in the past year, income, richest 60% (% age 15+)	Used the internet to pay bills or to buy something online in the past year, rural (% age 15+)	Used the internet to buy something online in the past year(% age 15+)	Used the internet to buy something online in the past year, male(% age 15+)	Used the internet to buy something online in the past year, in labor force (% age 15+)	Used the internet to buy something online in the past year, out of labor force (% age 15+)	Used the internet to buy something online in the past year, female(% age 15+)	Used the internet to buy something online in the past year, young adults (% age 15-24)	Used the internet to buy something online in the past year, older adults (% age 25+)	Used the internet to buy something online in the past year, primary education or less (% age 15+)	Used the internet to buy something online in the past year, secondary education or more (% age 15+)	Used the internet to buy something online in the past year, income, poorest 40% (% age 15+)	Used the internet to buy something online in the past year, income, richest 60% (% age 15+)	Used the internet to buy something online in the past year, rural (% age 15+)	Paid online for internet purchase (% internet purchasers, age 15+)	Paid cash on delivery for internet purchase (% internet purchasers, age 15+)	Saved to start, operate, or expand a farm or business (% age 15+) 	Saved to start, operate, or expand a farm or business, male (% age 15+) 	Saved to start, operate, or expand a farm or business, in labor force (% age 15+) 	Saved to start, operate, or expand a farm or business, out of labor force  (% age 15+) 	Saved to start, operate, or expand a farm or business, female (% age 15+) 	Saved to start, operate, or expand a farm or business, young adults (% age 15-24) 	Saved to start, operate, or expand a farm or business, older adults (% age 25+) 	Saved to start, operate, or expand a farm or business, primary education or less (% age 15+) 	Saved to start, operate, or expand a farm or business, secondary education or less(% age 15+) 	Saved to start, operate, or expand a farm or business, income, poorest 40% (% age 15+) 	Saved to start, operate, or expand a farm or business, income, richest 60% (% age 15+) 	Saved to start, operate, or expand a farm or business, rural (% age 15+) 	Saved for old age (% age 15+) 	Saved for old age, male (% age 15+) 	Saved for old age, in labor force (% age 15+) 	Saved for old age, out of labor force (% age 15+) 	Saved for old age, female (% age 15+) 	Saved for old age, young adults  (% age 15-24) 	Saved for old age,older adults (% age 25+) 	Saved for old age, primary education or less (% age 15+) 	Saved for old age, secondary education or more (% age 15+) 	Saved for old age, income, poorest 40% (% age 15+) 	Saved for old age, income, richest 60% (% age 15+) 	Saved for old age, rural (% age 15+) 	Saved at a financial institution (% age 15+) 	Saved at a financial institution, male (% age 15+) 	Saved at a financial institution, in labor force (% age 15+) 	Saved at a financial institution , out of labor force (% age 15+) 	Saved at a financial institution, female (% age 15+) 	Saved at a financial institution, young adults (% age 15-24) 	Saved at a financial institution, older adults (% age 25+) 	Saved at a financial institution, primary education or less(% age 15+) 	Saved at a financial institution, secondary education or more (% age 15+) 	Saved at a financial institution, income, poorest 40% (% age 15+) 	Saved at a financial institution, income, richest 60%  (% age 15+) 	Saved at a financial institution, rural  (% age 15+) 	Saved using a savings club or a person outside the family (% age 15+) 	Saved using a savings club or a person outside the family, male (% age 15+) 	Saved using a savings club or a person outside the family , in labor force(% age 15+) 	Saved using a savings club or a person outside the family, out of labor force (% age 15+) 	Saved using a savings club or a person outside the family, female (% age 15+) 	Saved using a savings club or a person outside the family, young adults (% age 15-24) 	Saved using a savings club or a person outside the family, older adults (% age 25+) 	Saved using a savings club or a person outside the family, primary education or less (% age 15+) 	Saved using a savings club or a person outside the family, secondary education or more (% age 15+) 	Saved using a savings club or a person outside the family, income, poorest 40%(% age 15+) 	Saved using a savings club or a person outside the family, income, richest 60% (% age 15+) 	Saved using a savings club or a person outside the family, rural (% age 15+) 	Saved for education or school fees (% age 15+) 	Saved for education or school fees, male (% age 15+) 	Saved for education or school fees, in labor force (% age 15+) 	Saved for education or school fees, out of labor force (% age 15+) 	Saved for education or school fees, female  (% age 15+) 	Saved for education or school fees , young adults (% age 15-24) 	Saved for education or school fees , older adults (% age 25+) 	Saved for education or school fees, primary education or less (% age 15+) 	Saved for education or school fees, secondary education or more (% age 15+) 	Saved for education or school fees, income, poorest 40%(% age 15+) 	Saved for education or school fees, income, richest 60% (% age 15+) 	Saved for education or school fees, rural  (% age 15+) 	Saved any money in the past year (% age 15+) 	Saved any money in the past year, male  (% age 15+) 	Saved any money in the past year, in labor force  (% age 15+) 	Saved any money in the past year, out of labor force (% age 15+) 	Saved any money in the past year, female  (% age 15+) 	Saved any money in the past year, young adults  (% age 15-24) 	Saved any money in the past year, older adults  (% age 25+) 	Saved any money in the past year, primary education or less (% age 15+) 	Saved any money in the past year, secondary education or more (% age 15+) 	Saved any money in the past year, income, poorest 40%(% age 15+) 	Saved any money in the past year, income, richest 60% (% age 15+) 	Saved any money in the past year, rural  (% age 15+) 	Outstanding housing loan (% age 15+) 	Outstanding housing loan, male  (% age 15+) 	Outstanding housing loan, in labor force (% age 15+) 	Outstanding housing loan, out of labor force (% age 15+) 	Outstanding housing loan, female (% age 15+) 	Outstanding housing loan, young adults (% age 15-24) 	Outstanding housing loan, older adults (% age 25+) 	Outstanding housing loan, primary education or less (% age 15+) 	Outstanding housing loan, secondary education or more(% age 15+) 	Outstanding housing loan, income, poorest 40% (% age 15+) 	Outstanding housing loan, income, richest 60% (% age 15+) 	Outstanding housing loan, rural  (% age 15+) 	Debit card ownership (% age 15+) 	Debit card ownership, male  (% age 15+) 	Debit card ownership, in labor force (% age 15+) 	Debit card ownership, out of labor force (% age 15+) 	Debit card ownership, female (% age 15+) 	Debit card ownership, young adults (% age 15-24) 	Debit card ownership, older adults (% age 25+) 	Debit card ownership, primary education or less (% age 15+) 	Debit card ownership, secondary education or more (% age 15+) 	Debit card ownership, income, poorest 40% (% age 15+) 	Debit card ownership, income, richest 60% (% age 15+) 	Debit card ownership, rural (% age 15+) 	Borrowed for health or medical purposes (% age 15+) 	Borrowed for health or medical purposes, male  (% age 15+) 	Borrowed for health or medical purposes , in labor force  (% age 15+) 	Borrowed for health or medical purposes, out of labor force (% age 15+) 	Borrowed for health or medical purposes, female  (% age 15+) 	Borrowed for health or medical purposes, young adults (% age 15-24) 	Borrowed for health or medical purposes, older adults (% age 25+) 	Borrowed for health or medical purposes, primary education or less (% age 15+) 	Borrowed for health or medical purposes, secondary education or more  (% age 15+) 	Borrowed for health or medical purposes, income, poorest 40% (% age 15+) 	Borrowed for health or medical purposes, income, richest 60%  (% age 15+) 	Borrowed for health or medical purposes, rural  (% age 15+) 	Borrowed to start, operate, or expand a farm or business (% age 15+) 	Borrowed to start, operate, or expand a farm or business, male (% age 15+) 	Borrowed to start, operate, or expand a farm or business, in labor force  (% age 15+) 	Borrowed to start, operate, or expand a farm or business, out of labor force (% age 15+) 	Borrowed to start, operate, or expand a farm or business, female (% age 15+) 	Borrowed to start, operate, or expand a farm or business, young adults  (% age 15-24) 	Borrowed to start, operate, or expand a farm or business, older adults (% age 25+) 	Borrowed to start, operate, or expand a farm or busines, primary education or less (% age 15+) 	Borrowed to start, operate, or expand a farm or business, secondary education or more (% age 15+) 	Borrowed to start, operate, or expand a farm or business, income, poorest 40%  (% age 15+) 	Borrowed to start, operate, or expand a farm or business, income, richest 60% (% age 15+) 	Borrowed to start, operate, or expand a farm or business, rural (% age 15+) 	Borrowed from a store by buying on credit (% age 15+) 	Borrowed from a store by buying on credit, male (% age 15+) 	Borrowed from a store by buying on credit, in labor force (% age 15+) 	Borrowed from a store by buying on credit, out of labor force (% age 15+) 	Borrowed from a store by buying on credit, female (% age 15+) 	Borrowed from a store by buying on credit, young adults (% age 15-24) 	Borrowed from a store by buying on credit, older adults (% age 25+) 	Borrowed from a store by buying on credit, primary education or less (% age 15+) 	Borrowed from a store by buying on credit, secondary education or more (% age 15+) 	Borrowed from a store by buying on credit, income, poorest 40%  (% age 15+) 	Borrowed from a store by buying on credit, income, richest 60% (% age 15+) 	Borrowed from a store by buying on credit, rural  (% age 15+) 	Borrowed for education or school fees (% age 15+) 	Borrowed for education or school fees, male  (% age 15+) 	Borrowed for education or school fees, in labor force  (% age 15+) 	Borrowed for education or school fees , out of labor force (% age 15+) 	Borrowed for education or school fees, female  (% age 15+) 	Borrowed for education or school fees, young adults  (% age 15-24) 	Borrowed for education or school fees, older adults  (% age 25+) 	Borrowed for education or school fees, primary education or less (% age 15+) 	Borrowed for education or school fees, secondary education or more (% age 15+) 	Borrowed for education or school fees, income, poorest 40 %(% age 15+) 	Borrowed for education or school fees, income, richest 60% (% age 15+) 	Borrowed for education or school fees, rural  (% age 15+) 	Borrowed from a financial institution (% age 15+) 	Borrowed from a financial institution, male (% age 15+) 	Borrowed from a financial institution, in labor force (% age 15+) 	Borrowed from a financial institution, out of labor force (% age 15+) 	Borrowed from a financial institution, female (% age 15+) 	Borrowed from a financial institution, young adults  (% age 15-24) 	Borrowed from a financial institution, older adults  (% age 25+) 	Borrowed from a financial institution, primary education or less (% age 15+) 	Borrowed from a financial institution, secondary education or more (% age 15+) 	Borrowed from a financial institution, income, poorest 40% (% age 15+) 	Borrowed from a financial institution, income, richest 60% (% age 15+) 	Borrowed from a financial institution, rural (% age 15+) 	Borrowed from a financial institution or used a credit card (% age 15+) 	Borrowed from a financial institution or used a credit card, male (% age 15+) 	Borrowed from a financial institution or used a credit card, in labor force (% age 15+) 	Borrowed from a financial institution or used a credit card, out of labor force (% age 15+) 	Borrowed from a financial institution or used a credit card, female (% age 15+) 	Borrowed from a financial institution or used a credit card, young adults (% age 15-24) 	Borrowed from a financial institution or used a credit card, older adults (% age 25+) 	Borrowed from a financial institution or used a credit card, primary education or less (% age 15+) 	Borrowed from a financial institution or used a credit card, secondary education or more (% age 15+) 	Borrowed from a financial institution or used a credit card, income, poorest 40% (% age 15+) 	Borrowed from a financial institution or used a credit card, income, richest 60% (% age 15+) 	Borrowed from a financial institution or used a credit card, rural (% age 15+) 	Borrowed from family or friends (% age 15+) 

merchant_name                VARCHAR(MAX),
cash_in_count                INTEGER,
cash_out_count               INTEGER,
payment_count                INTEGER,
distinct_customers_count     INTEGER
);
"""

INSERT_INTO_MERCHANTS_TABLE_SQL = """
INSERT INTO merchants (merchant_name, cash_in_count, cash_out_count, payment_count, distinct_customers_count) 
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
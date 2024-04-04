--DROP TABLE IF EXISTS stv2023111354__STAGING.transactions CASCADE;
CREATE TABLE IF NOT EXISTS stv2023111354__STAGING.transactions
    (operation_id varchar(60), 
    account_number_from int,
    account_number_to int,
    currency_code int,
    country varchar(30),
    status varchar(30),
    transaction_type varchar(30),
    amount int,
    transaction_dt timestamp)
    PARTITION BY transaction_dt::date;
   
   
CREATE PROJECTION stv2023111354__STAGING.transactions_projection 
    AS SELECT * FROM stv2023111354__STAGING.transactions
    ORDER BY transaction_dt
    SEGMENTED BY hash(operation_id,transaction_dt) ALL NODES;



--DROP TABLE IF EXISTS stv2023111354__STAGING.currencies CASCADE;  
CREATE TABLE IF NOT EXISTS stv2023111354__STAGING.currencies
    (date_update timestamp, 
    currency_code int,
    currency_code_with int,
    currency_with_div numeric(5,3))
    PARTITION BY date_update::date;


CREATE PROJECTION stv2023111354__STAGING.currencies
    AS SELECT * FROM stv2023111354__STAGING.currencies
    ORDER BY date_update
    SEGMENTED BY hash(currency_code,date_update) ALL NODES;
    
   
--DROP TABLE IF EXISTS stv2023111354__DWH.global_metrics;   
CREATE TABLE IF NOT EXISTS stv2023111354__dwh.global_metrics
    (
        date_update date,
        currency_from int,
        amount_total numeric(18,6),
        cnt_transactions int,
        avg_transactions_per_account numeric(18,6),
        cnt_accounts_make_transactions int
    );
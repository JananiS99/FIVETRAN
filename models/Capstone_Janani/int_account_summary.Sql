WITH TRANSACTION_DATA AS (
    SELECT
        t.account_id,
        sum(case when t.transaction_type = 'deposit' then t.amount else 0 end) as total_deposits,
        sum(case when t.transaction_type = 'withdrawal' then t.amount else 0 end) as total_withdrawals,
        count(*) as transaction_count
    FROM STG_TRANSACTIONS t
    GROUP BY t.account_id
),

ACCOUNT_SUMMARY AS (
    SELECT
        td.account_id,
        td.total_deposits,
        td.total_withdrawals,
        td.transaction_count,
        b.balance as banking_balance,
        (td.total_deposits - td.total_withdrawals) as calculated_balance,
        b.customer_id,
        CASE
            when (td.total_deposits - td.total_withdrawals) = b.balance then true
            else false
        end as balance_match
    FROM transaction_data td
    JOIN banking b ON td.account_id = b.account_id
)

SELECT * FROM ACCOUNT_SUMMARY;
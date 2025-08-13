WITH CUSTOMER_METRICS AS (
    SELECT
        a.customer_id,
        a.calculated_balance,
        SUM(t.amount) AS total_transaction_amount,
        COUNT(t.transaction_id) AS transaction_count
    FROM INT_ACCOUNT_SUMMARY a
    JOIN INT_CUSTOMER_TRANSACTION c ON a.customer_id=c.customer_id
    JOIN STG_TRANSACTIONS t
    ON a.account_id = t.account_id
    GROUP BY a.customer_id, a.calculated_balance
)

SELECT *
FROM CUSTOMER_METRICS
WHERE total_transaction_amount > 10000
AND calculated_balance > 5000


WITH BASE_DATA AS (
    SELECT
        DATE_TRUNC('MONTH', transaction_date) AS year_month,
        COUNT(*) AS transaction_volume,
        AVG(amount) AS avg_transaction_amount
    FROM CAP_CAPSTONE_FIVETRAN_JAN.STG_TRANSACTIONS
    WHERE transaction_date > '2025-01-01'  -- Replace with appropriate date
    GROUP BY DATE_TRUNC('MONTH', transaction_date)
)

SELECT * FROM BASE_DATA
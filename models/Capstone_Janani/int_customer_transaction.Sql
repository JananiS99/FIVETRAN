WITH CLEANED AS (
    SELECT I.customer_id, ST.transaction_date, ST.amount
    FROM INT_ACCOUNT_SUMMARY I 
    JOIN STG_TRANSACTIONS ST
),
BANK AS (
    SELECT customer_id, status
    FROM BANKING
    WHERE status = 'Active'
),
AGGREGATED AS (
    SELECT
        c.customer_id,
        MAX(c.transaction_date) AS most_recent_transaction_date,
        SUM(c.amount) AS total_transaction_amount
    FROM CLEANED c
    JOIN bank b ON c.customer_id = b.customer_id
    GROUP BY c.customer_id
)

SELECT * FROM AGGREGATED;


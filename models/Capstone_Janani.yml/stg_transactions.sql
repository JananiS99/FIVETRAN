
WITH DEDUPLICATED AS (
    SELECT *
    FROM (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY created_at DESC) AS row_num
        FROM TRANSACTIONS
        WHERE _fivetran_deleted = FALSE
    )
    WHERE row_num = 1
),

CLEANED AS (
    SELECT
        transaction_id,
        account_id,
        transaction_date,
        amount,
        INITCAP(transaction_type) AS transaction_type,
        CASE 
            WHEN description IS NULL OR TRIM(description) = '' THEN 'No Values'
            ELSE description
        END AS description,
        created_at,
        _fivetran_synced
    FROM DEDUPLICATED
)

SELECT * FROM CLEANED

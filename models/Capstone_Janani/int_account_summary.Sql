with transaction_data as (
    select
        t.account_id,
        sum(case when t.transaction_type = 'deposit' then t.amount else 0 end) as total_deposits,
        sum(case when t.transaction_type = 'withdrawal' then t.amount else 0 end) as total_withdrawals,
        count(*) as transaction_count
    from transactions t
    group by t.account_id
),

account_summary as (
    select
        td.account_id,
        td.total_deposits,
        td.total_withdrawals,
        td.transaction_count,
        b.balance as banking_balance,
        (td.total_deposits - td.total_withdrawals) as calculated_balance,
        case
            when (td.total_deposits - td.total_withdrawals) = b.balance then true
            else false
        end as balance_match
    from transaction_data td
    join banking b on td.account_id = b.account_id
)

select * from account_summary

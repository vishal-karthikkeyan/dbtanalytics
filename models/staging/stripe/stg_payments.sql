with source as (

    select * from {{ source('stripe', 'payment') }}
    
),
 transformed as (
    select 
    ORDERID as order_id, max(CREATED) as payment_finalized_date, 
    sum(AMOUNT) / 100.0 as total_amount_paid
    from source
    where STATUS <> 'fail'
    group by 1
)
select * from transformed
WITH 

customer as(
    select * from {{ ref('stg_customers') }}
),
orders as(
    select * from {{ ref('stg_orders') }}
),
payments as(
    select * from {{ ref('stg_payments') }}
),



paid_orders as (
    select 
    Orders.ID as order_id,
    Orders.USER_ID	as customer_id,
    Orders.ORDER_DATE as order_placed_at,
    Orders.STATUS as order_status,
    p.total_amount_paid,
    p.payment_finalized_date,
    C.FIRST_NAME    as customer_first_name,
    C.LAST_NAME as customer_last_name
from orders
left join payments p on orders.ID = p.order_id
left join customer C on orders.USER_ID = C.ID 
),


customer_orders as (
    select 
    C.ID as customer_id
    , min(ORDER_DATE) as first_order_date
    , max(ORDER_DATE) as most_recent_order_date
    , count(ORDERS.ID) as number_of_orders
    from customer C 
    left join orders as Orders
    on orders.USER_ID = C.ID 
    group by 1
),

clv as (

    select
        p.order_id,
        sum(t2.total_amount_paid) as customer_lifetime_value
    from paid_orders p
    left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
)

--final
select
    p.*,
    ROW_NUMBER() OVER (order by p.order_id) as transaction_seq,
    ROW_NUMBER() OVER (PARTITIon BY customer_id order by p.order_id) as customer_sales_seq,
    CASE 
        WHEN c.first_order_date = p.order_placed_at
        THEN 'new'
        ELSE 'return' 
    END as nvsr,
    x.customer_lifetime_value,
    c.first_order_date as fdos
from paid_orders p
left join customer_orders as c USING (customer_id)
LEFT OUTER JOIN 
clv x on x.order_id = p.order_id
order by order_id
with source as(

    select * from {{ source('jaffle_shop', 'orders') }}
    
),
 transformed as (
    select * from source
)
select * from transformed
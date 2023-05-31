with source as(

    select * from {{ source('jaffle_shop', 'customers') }}
    
),
 transformed as (
    select * from source
)
select * from transformed
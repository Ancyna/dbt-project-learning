with source as(
    select * from {{ source('jaffle_shop', 'customers') }}
),

tansformed AS(
      select 
        id as customer_id
        ,last_name as surname,
        ,first_name as givenname
        ,first_name || ' ' || last_name as full_name
      from  source --raw.jaffle_shop.customers
)

select * from tansformed
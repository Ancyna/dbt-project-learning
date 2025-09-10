with source as (
    select * from {{ source('stripe', 'payment') }}
),

tansformed AS(
    select
        ,id as payment_id
        ,orderid as order_id
        ,status as payment_status
        ,payment.payment_amount as payment_amount
    from source
)

select * from tansformed
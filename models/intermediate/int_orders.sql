WITH
orders AS(
    select
        *
    from {{ ref('stg_jaffle_shop__orders') }}
),
payments AS(
    select
        *
    from {{ ref('stg_stripe__payments') }}
    where payment_status != 'fail'
),
order_totals AS(
    select
        order_id
        ,payment_status
        ,sum(payment_amount) as order_value_dollars
    from payment --raw.stripe.payment c
    group by 1,2        
),
order_value_joined AS(
    select
        order.*
        ,order_totals.payment_status
        ,order_totals.order_value_dollars
    from orders
    left join order_totals
        on orders.order_id = order_totals.order_id
)

select * from order_value_joined
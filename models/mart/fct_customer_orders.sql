-- with statement
WITH
-- import CTEs
customers AS(
    select  
        * 
    from  {{ ref('stg_jaffle_shop__customers') }}
),
orders AS(
    select * from {{ ref('int_orders') }}
)
,

final AS(
    select

    from orders
    inner join customers
        on orders.customer_id = customers.customer_id
)




customer_order_history AS (

    select 
        customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname,
        min(orders.order_date) as first_order_date,
        min(orders.valid_order_date) as first_non_returned_order_date,
        max(orders.valid_order_date) as most_recent_non_returned_order_date,
        COALESCE(max(user_order_seq),0) as order_count,
        COALESCE(count(case 
                        when orders.valid_order_date is not null 
                        then 1 end)
                ,0) as non_returned_order_count,
        sum(case 
                when orders.valid_order_date is not null 
                    then orders.order_value_dollars 
                else 0 
                end
            ) as total_lifetime_value,
        sum(case 
                when orders.valid_order_date is not null 
                    then orders.order_value_dollars
                else 0 
                end
            )/NULLIF(count(case 
                            when orders.valid_order_date is not null 
                            then 1 end)
                    ,0) as avg_non_returned_order_value,
        array_agg(distinct orders.order_id) as order_ids

    from  customers
    
    join  orders
        on orders.customer_id = customers.customer_id

    

    group by customers.id, customers.name, customers.last_name, customers.first_name

),

-- final CTE
final as (
    select 
        orders.order_id,
        orders.customer_id,
        customer_order_history.surname,
        customer_order_history.givenname,
        customer_order_history.first_order_date,
        customer_order_history.order_count,
        customer_order_history.total_lifetime_value,
        orders.order_value_dollars,
        orders.order_status,
        payments.payment_status
    from orders --raw.jaffle_shop.orders as orders

    join customers
        on orders.customer_id = customers.id

    join  customer_order_history
        on orders.customer_id = customer_order_history.customer_id

    left outer join payments --raw.stripe.payment payments
        on orders.order_id = payments.order_id
    --where payments.payment_status != 'fail'
)

-- simple select statement

select * from final


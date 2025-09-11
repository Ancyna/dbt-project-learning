with payment as(
    select * 
    from {{ ref('stg_stripe__payments') }}
    where status = 'success'
),
pivoted as(
    select
        order_id,

        {%- set payment_methods= ['credit_card','bank_transfer','gift_card','coupon'] -%}
        {% for payment_method in payment_methods  %}
            sum(case when payment_method = '{{payment_method}}' then amount else 0 end) as {{payment_method}}_amount
            
            {%- if not loop.last -%}
                ,                
            {%- endif -%}
            
        {% endfor %}

    from payment
    group by order_id
)

select * from pivoted
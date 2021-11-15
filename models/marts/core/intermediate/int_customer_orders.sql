with customer_orders as (
    select
        customers.id as customer_id,
        customers.full_name,
        dense_rank() over (partition by customers.id order by orders.created_at) as customer_order_count,
        orders.id as order_id,
        orders.created_at as order_date,
        products.product_name,
        products.category,
        product_prices.price
    

    from {{ ref('stg_orders') }} orders
        left join {{ ref('stg_order_items') }} order_items on orders.id = order_items.order_id
        left join {{ ref('stg_products') }} products on order_items.product_id = products.id
        left join {{ ref ('stg_product_prices') }} product_prices
            on products.id = product_prices.product_id
            and orders.created_at between product_prices.created_at and product_prices.ended_at
        left join {{ ref('stg_customers') }} customers on orders.customer_id = customers.id
)

select *
from customer_orders
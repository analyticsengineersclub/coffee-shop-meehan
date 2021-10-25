{{ config(materialized='table') }}

with customer_orders as (
    select
        customer_id,
        count(distinct orders.id) as number_of_orders,
        min(created_at) as first_order_at
    from {{ ref('stg_orders') }} orders
    group by customer_id
)

select
    customers.id as customer_id,
    customers.full_name,
    customers.email,
    customer_orders.first_order_at,
    customer_orders.number_of_orders

from {{ ref('stg_customers') }} customers
    left join customer_orders on customers.id = customer_orders.customer_id
select
    customer_id,
    full_name,
    case
        when customer_order_count > 1 then false
        else true
    end as new_customer,
    product_name,
    category,
    price,
    order_id,
    date_trunc(order_date, week) as order_week

from {{ ref('int_customer_orders') }}
with days_as_customer as (
    select
        customer_id,
        row_number() over (partition by customer_id order by calendar.date_day) as total_days
    
    from {{ ref('dim_customers') }} customers
        cross join {{ ref('calendar_days') }} calendar
    where calendar.date_day >= cast(customers.first_order_at as date)
),

weeks_as_customer as (
    select
        customer_id,
        ceiling(cast(total_days as numeric) / 7.0) as week
    from days_as_customer
    group by customer_id, week
),

customer_orders as (
    select
        orders.customer_id,
        case
            when date_diff(orders.created_at, customers.first_order_at, day) = 0 then 1 -- customers first order should be represented as week 1 of being a customer
            else ceiling(cast(date_diff(orders.created_at, customers.first_order_at, day) as numeric) / 7.0) -- ie. 17 days → 2.4 wks → ceiling(2.4) = week 3 as customer
        end as order_week,
        sum(total) as revenue
    from {{ ref('stg_orders') }} orders
        left join {{ ref('dim_customers') }} customers on orders.customer_id = customers.customer_id
    group by customer_id, order_week
)

select
    weeks_as_customer.customer_id,
    week,
    coalesce(revenue, 0) as revenue,
    sum(coalesce(revenue, 0)) over (partition by weeks_as_customer.customer_id order by week) as cumulative_revenue
from weeks_as_customer
    left join customer_orders on weeks_as_customer.customer_id = customer_orders.customer_id
        and weeks_as_customer.week = customer_orders.order_week

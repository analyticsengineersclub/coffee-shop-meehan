with calendar as (
    select *
    from {{ ref('calendar_weeks') }}
),

weeks_as_customer as (
    select
        customer_id,
        row_number() over (partition by customer_id order by calendar.date_week) as week,
        calendar.date_week
    
    from {{ ref('dim_customers') }} customers
        cross join calendar
    where calendar.date_week >= cast(date_trunc(customers.first_order_at, week) as date)
),

customer_orders as (
    select
        customer_id,
        cast(date_trunc(created_at, week) as date) as order_week,
        sum(total) as revenue
    from {{ ref('stg_orders') }}
    group by customer_id, order_week
)

select
    weeks_as_customer.customer_id,
    week,
    coalesce(revenue, 0) as revenue,
    sum(coalesce(revenue, 0)) over (partition by weeks_as_customer.customer_id order by week) as cumulative_revenue
from weeks_as_customer
    left join customer_orders on weeks_as_customer.customer_id = customer_orders.customer_id
        and weeks_as_customer.date_week = customer_orders.order_week

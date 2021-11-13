select
  date_trunc(order_date, month) as date_month,
  sum(case when category = 'coffee beans' then price end) as coffee_beans_revenue,
  sum(case when category = 'merch' then price end) as merch_revenue,
  sum(case when category = 'brewing supplies' then price end) as brewing_supplies_revenue
from {{ ref('int_customer_orders') }}
group by 1
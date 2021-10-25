select
  date_trunc(first_order_at, month) as month,
  count(*) as customer_count

from {{ ref('stg_customers') }}

group by 1
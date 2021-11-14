select
  date_trunc(order_date, month) as date_month,
  {% for category in get_product_categories() %}
  sum(case when category = '{{category}}' then price end) as {{category|replace(" ", "_")}}_revenue,
  -- ^replace() is necessary to handle whitespace in coffee beans and brewing supplies for column naming
  {% endfor %}
from {{ ref('int_customer_orders') }}
group by 1
{% test single_visitor_id_per_customer(model, column_name) %}

select
    customer_id,
    count(distinct {{ column_name }} ) as total_visitor_ids
from {{ model }}
where customer_id is not null
group by customer_id
having total_visitor_ids > 1
-- ^ if there is a customer_id with more than 1 visitor_id tied to it, a row will be returned and cause this test to fail

{% endtest %}
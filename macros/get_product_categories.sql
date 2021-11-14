{% macro get_product_categories() %}

{{ return(get_column_values('category', ref('int_customer_orders'))) }}

{% endmacro %}


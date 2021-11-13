{% macro get_product_categories () %}

-- define a query statement to dynamically return all product categories
{% set find_categories %}
    select distinct
        category
    from {{ ref('int_customer_orders')}}
{% endset %}

-- run the query using dbt run_query() macro
-- this will return an agate table where category is a single row
{% set categories = run_query(find_categories) %}
{# {{ log(categories, info=true) }} #}
-- ^ this is an agate table

-- apply additional agate style transformation to turn category column into a list of values
-- execute is a dbt jinja command that returns true when dbt is in "execute mode" (ie. dbt compile, dbt run)
{% if execute %}
    {% set categories_list = categories.columns[0].values() %}
    -- ^ pulls all values out of anything in the first column (ie.category)
{% else %}
    {% set categories_list = [] %}
{% endif %}

{# {{ log(categories_list, info=true) }} #}
{{ return(categories_list) }}
{% endmacro %}}

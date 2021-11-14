{% macro get_column_values(column_name, table_name) %}

-- define a query statement to dynamically return all product categories
{% set find_values %}
    select distinct
        {{ column_name }}
    from {{ table_name }}
{% endset %}

-- run the query using dbt run_query() macro
-- this will return an agate table where the column_name passed in is a single row
{% set results = run_query(find_values) %}
-- ^ this is an agate table

-- apply additional agate style transformation to turn category column into a list of values
-- execute is a dbt jinja command that returns true when dbt is in "execute mode" (ie. dbt compile, dbt run)
{% if execute %}
    {% set results_list = results.columns[0].values() %}
    -- ^ pulls all values out of anything in the first column (ie. column_name passed in)
{% else %}
    {% set results_list = [] %}
{% endif %}

{# {{ log(results_list, info=true) }} #}
{{ return(results_list) }}
{% endmacro %}}
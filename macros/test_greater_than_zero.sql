{% test greater_than_zero(model, column_name) %}
    select
        {{ column_name }}
        
    from {{ model }}
	where not ( {{ column_name }} > 0 )
{% endtest %}
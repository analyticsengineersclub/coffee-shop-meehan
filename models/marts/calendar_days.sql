with calendar as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2021-01-01' as date)",
        end_date="cast(current_date() as date)"
        )
    }}
)

select *
from calendar
order by date_day
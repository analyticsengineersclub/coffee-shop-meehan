with calendar as (
    {{ dbt_utils.date_spine(
        datepart="week",
        start_date="cast(date_trunc('2021-01-01', week) as date)",
        end_date="cast(current_date() as date)"
        )
    }}
)

select *
from calendar
order by date_week
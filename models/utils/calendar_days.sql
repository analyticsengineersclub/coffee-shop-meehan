{{ config(materialized='table') }}

with calendar as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2021-11-14' as date)",
        end_date="cast(current_date()+1 as date)"
        )
    }}
)

-- ^end_date isn't included so adding +1 so that this table contains the current_date()

select *
from calendar
order by date_day
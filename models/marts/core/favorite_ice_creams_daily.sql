select
    github_username,
    favorite_ice_cream_flavor,
    date_day

from {{ ref('calendar_days') }} calendar_days
    join {{ ref('favorite_ice_cream_flavors') }} favorite_flavors on
        calendar_days.date_day >= cast(favorite_flavors.dbt_valid_from as date)
        and calendar_days.date_day < cast(coalesce(favorite_flavors.dbt_valid_to, cast(current_date()+1 as timestamp))  as date)
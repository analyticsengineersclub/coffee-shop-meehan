{{ config(
    materialized='incremental',
    unique_key='github_username' 
) }}
-- the `unique_key` tells dbt how to aggregate records so that the incremental model doesn't create duplicates when it updates

with events as (
    select * from {{ source('advanced_dbt_examples', 'form_events') }}

    --the below is necessary for incremental models to tell dbt how to delineate what the new records are from the last run
    -- it's a good idea to account for data that could arrive a bit late
    {% if is_incremental() %}
    where github_username in ( -- <- this ensures that previous records are not dropped from the model
        select distinct github_username from {{ source('advanced_dbt_examples', 'form_events') }}
        where timestamp > (select date_add(max(last_form_entry), interval -1 hour) from {{this}})
    )
    {% endif %}

    -- where {{this}} represents the currently existing object mapped to this model
    -- it's important that {{this}} is wrapped in an if statement:
    -- because if this was the first run of the model it would get materialized as a table and {{this}} doesn't exist yet
),

aggregated as (
    select
        github_username,
        min(timestamp) as first_form_entry,
        max(timestamp) as last_form_entry,
        count(*) as number_of_entries
    from events
    group by 1
)

select * from aggregated
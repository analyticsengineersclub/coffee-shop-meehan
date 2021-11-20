{{ config(
    materialized='incremental'
) }}

with events as (
    select * from {{ source('advanced_dbt_examples', 'form_events') }}

    --the below is necessary for incremental models to tell dbt how to delineate what the new records are from the last run
    {% if is_incremental() %}
    where timestamp >= (select max(last_form_entry) from {{this}})
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
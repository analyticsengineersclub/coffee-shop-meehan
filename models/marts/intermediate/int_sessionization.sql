{{ config(
    materialized='table',
    cluster_by=['customer_id', 'device_type', 'page']
) }}

with session_splitter as (
    select
        *,
        case
            when timestamp_diff(occured_at, last_event, minute) > 30
                or ( last_event is null ) then 1
            else 0
        end as is_new_session
    
    from (
        select 
            id,
            visitor_id,
            device_type,
            timestamp as occured_at,
            lag(timestamp) over (partition by visitor_id order by timestamp asc) as last_event,
            page,
            customer_id
        from {{ ref('int_user_stitching') }}
    ) log_previous
),

event_mapper as (
    select
        *,
        sum(is_new_session) over (order by visitor_id, occured_at) as global_session_id,
        sum(is_new_session) over (partition by visitor_id order by occured_at) as visitor_session_number

    from session_splitter
),

start_stop as (
    select 
        global_session_id,
        min(occured_at) as session_start,
        max(occured_at) as session_end
    from event_mapper 
    group by global_session_id
),

sessionization as (
    select
        event_mapper.id,
        event_mapper.visitor_id,
        event_mapper.device_type,
        event_mapper.occured_at,
        event_mapper.page,
        event_mapper.global_session_id,
        event_mapper.visitor_session_number,
        event_mapper.customer_id,
        start_stop.session_start,
        start_stop.session_end 
        
    from event_mapper
        join start_stop on event_mapper.global_session_id = start_stop.global_session_id
)

select *
from sessionization
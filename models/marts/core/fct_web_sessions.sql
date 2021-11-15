with session_stats as (
    select
        global_session_id,
        timestamp_diff(session_end, session_start,second) as duration_in_seconds,
        count(page) as pages_visited,
        logical_or(case
            when page = 'order-confirmation' then true
            else false
        end) as purchase_made
    from {{ ref('int_sessionization') }}
    group by 1, 2
)

select *
from session_stats
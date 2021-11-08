with verified_customers as (
    select
        customer_id,
        min(visitor_id) as stitched_id
    from {{ ref('stg_pageviews') }}
    where customer_id is not null
    group by customer_id
)


select
    id,
    case
        when pageviews.customer_id is null then pageviews.visitor_id
        else verified_customers.stitched_id
    end as visitor_id,
    device_type,
    page,
    pageviews.customer_id,
    timestamp

from {{ ref('stg_pageviews') }} pageviews
    left join verified_customers on pageviews.customer_id = verified_customers.customer_id
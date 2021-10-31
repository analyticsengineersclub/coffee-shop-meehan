select
    id,
    visitor_id,
    device_type,
    page,
    customer_id,
    timestamp

from {{ source('web_tracking', 'pageviews') }}
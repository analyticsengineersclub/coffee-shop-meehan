select
    id,
    product_id,
    price,
    created_at,
    ended_at

from {{ source('coffee_shop', 'product_prices') }}
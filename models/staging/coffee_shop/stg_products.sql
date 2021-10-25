select
    id,
    name as product_name,
    category
    -- excluded columns
    -- created_at

from {{ source('coffee_shop', 'products') }}
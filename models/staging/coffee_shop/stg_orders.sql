select
    id,
    customer_id,
    total,
    -- timestamps
    created_at

    -- excluded columns
    -- address
    -- state
    -- zip

from {{ source('coffee_shop', 'orders') }}
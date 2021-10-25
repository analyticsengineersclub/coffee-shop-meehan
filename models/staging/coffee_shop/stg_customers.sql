select
    id,
    name as full_name,
    email

from {{ source('coffee_shop', 'customers') }}
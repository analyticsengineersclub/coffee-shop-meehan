{% snapshot favorite_ice_cream_flavors %}
-- ^ the name of the snapshot will come from this name vs the filename (dissimilar to models)
-- it's often good practice to give snapshots their own unique schema
{{ config(
      target_schema='dbt_meehan_snapshots',
      unique_key='github_username',
      strategy='timestamp',
      updated_at='updated_at'
) }}

select *
from {{ source('advanced_dbt_examples', 'favorite_ice_cream_flavors') }}

{% endsnapshot %}
with source as (
    select * from {{ source('rawg', 'rawg_parent_platform') }}
), 

stg_parent_platform as (
    select
        parent_platform_id,
        name as parent_platform_name,
        slug as parent_platform_slug,
        updated_at as parent_platform_updated_at,
        platform_id as parent_platform_platform_id
    from source
)


select * from stg_parent_platform
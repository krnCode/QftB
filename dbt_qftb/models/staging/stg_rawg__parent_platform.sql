with

source as (
    select * from {{ source('rawg', 'rawg_parent_platform') }}
),

stg_parent_platform as (
    select

        -- ids
        parent_platform_id,
        platform_id as parent_platform_platform_id, -- text[] array

        -- strings
        name as parent_platform_name,
        slug as parent_platform_slug,

        -- timestamps
        updated_at as parent_platform_updated_at

    from source
)


select * from stg_parent_platform

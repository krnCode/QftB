with

parent_platform as (
    select * from {{ ref('stg_rawg__parent_platform') }}
),

platforms as (
    select * from {{ ref('stg_rawg__platforms') }}
),

unnested_parent_platform as (
    select
        parent_platform_id,
        parent_platform_name,

        unnest(parent_platform_platform_id) as parent_platform_platform_id

    from parent_platform
),

joined_parent_platform as (
    select
        parent.parent_platform_id,
        parent.parent_platform_name,
        parent.parent_platform_platform_id,
        platform.platform_id,
        platform.platform_name,
        platform.platform_image

    from
        unnested_parent_platform as parent

    left join
        platforms as platform
        on parent.parent_platform_platform_id = platform.platform_id

    where
        platform.platform_id is not null
),

final_parent_platform as (
    select
        parent_platform_id,
        parent_platform_name,
        platform_id,
        platform_name,
        platform_image
    from joined_parent_platform
)

select * from final_parent_platform

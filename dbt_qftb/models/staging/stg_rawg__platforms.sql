with

source as (
    select * from {{ source('rawg', 'rawg_platforms') }}
),

column_renamed as (
    select
        -- ids
        platform_id,

        -- strings
        name as platform_name,
        slug as platform_slug,
        image as platform_image,

        -- numerics
        games_count as platform_games_count,
        year_start as platform_year_start,
        year_end as platform_year_end,

        -- timestamps
        updated_at as platform_updated_at
    from source
)

select * from column_renamed

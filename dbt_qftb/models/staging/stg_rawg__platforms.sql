with source as (
    select * from {{ source('rawg', 'rawg_platforms') }}
),

column_renamed as (
    select
        platform_id,
        name as platform_name,
        slug as platform_slug,
        games_count as platform_games_count,
        image as platform_image,
        year_start as platform_year_start,
        year_end as platform_year_end,
        updated_at as platform_updated_at
    from source
)

select * from column_renamed
with

source as (
    select * from {{ source('rawg', 'rawg_tags') }}
),

stg_tags as (
    select
        -- ids
        tag_id,

        -- strings
        name as tag_name,
        slug as tag_slug,
        image_background as tag_image_background,

        -- numerics
        games_count as total_rawg_game_count_in_tag,
        language as tag_language,

        -- timestamps
        updated_at

    from source
),

capitalize_tag_name as (
    select
        *,
        initcap(tag_name) as tag_name_capitalized

    from stg_tags
)

select * from capitalize_tag_name

with source as (
    select * from {{ source('rawg', 'rawg_tags') }}
),

stg_tags as (
    select
        tag_id,
        initcap(name) as tag_name,
        slug,
        games_count as total_rawg_game_count_in_tag,
        image_background,
        language,
        updated_at
    from source
)

select * from stg_tags
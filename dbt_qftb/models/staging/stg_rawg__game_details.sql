with source as (
    select * from {{ source("rawg", "rawg_game_details") }}
),

renamed as (
    select
        game_id,
        description_raw,
        tba,
        background_image,
        rating_overall,
        ratings_count,
        reviews_count,
        reviews_text_count,
        achievements_count,
        esrb_rating_id,
        genre_id,       -- bigint[] array
        tag_id,         -- bigint[] array
        developer_id,   -- bigint[] array
        publisher_id,   -- bigint[] array
        platform_id,    -- bigint[] array
        reddit_name,
        reddit_description,
        reddit_url,
        updated_on_rawg,
        updated_at
    from source
)

select * from renamed
with

source as (
    select * from {{ source("rawg", "rawg_game_details") }}
),

renamed as (
    select
        -- ids
        game_id,
        genre_id,       -- bigint[] array
        tag_id,         -- bigint[] array
        developer_id,   -- bigint[] array
        publisher_id,   -- bigint[] array
        platform_id,    -- bigint[] array
        esrb_rating_id,

        -- strings
        description_raw,
        background_image,
        reddit_name,
        reddit_description,
        reddit_url,

        -- numerics
        rating_overall,
        ratings_count,
        reviews_count,
        reviews_text_count,
        achievements_count,

        -- booleans
        tba,

        -- timestamps
        updated_on_rawg,
        updated_at

    from source
)

select * from renamed

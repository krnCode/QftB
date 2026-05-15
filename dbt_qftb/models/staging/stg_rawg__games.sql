with source as (
    select * from {{ source("rawg", "rawg_games") }}
),

renamed as (
    select
        -- ids
        game_id,

        -- strings
        slug as game_slug,
        name as game_name,
        platforms as game_platform,   -- text[] array
        genres as game_genre,      -- text[] array

        -- numerics
        rating as game_rating,
        ratings_count as game_ratings_count,

        -- dates
        released as game_date_released,

        -- timestamps
        updated_at

    from source
)

select * from renamed

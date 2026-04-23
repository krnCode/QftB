with source as (
    select * from {{ source("rawg", "rawg_games") }}
),

renamed as (
    select
        game_id,
        slug,
        name,
        released,
        rating,
        ratings_count,
        platforms,   -- text[] array
        genres,      -- text[] array
        updated_at
    from source
)

select * from renamed
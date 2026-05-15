with

games as (
    select * from {{ ref("stg_rawg__games") }}
),

details as (
    select * from {{ ref("stg_rawg__game_details") }}
),

final as (
    select
        -- ids
        g.game_id,
        d.platform_id,
        d.esrb_rating_id,
        d.developer_id,
        d.publisher_id,
        d.genre_id,
        d.tag_id,

        -- strings
        g.game_slug,
        g.game_name,
        d.description_raw,
        d.background_image,
        d.reddit_name,
        d.reddit_url,

        -- numerics
        g.game_rating,
        g.game_ratings_count,
        d.rating_overall,
        d.reviews_count,
        d.reviews_text_count,
        d.achievements_count,

        -- booleans
        d.tba,

        -- dates
        g.game_date_released,

        -- timestamps
        d.updated_on_rawg,
        g.updated_at

    from games as g

    left join
        details as d
        using (game_id)
)

select * from final

with games as (
    select * from {{ ref("stg_rawg__games") }}
),

details as (
    select * from {{ ref("stg_rawg__game_details") }}
),

final as (
    select
        -- identifiers
        g.game_id,
        g.slug,
        g.name,
        d.description_raw,

        -- release info
        g.released,
        d.tba,

        -- ratings and reviews
        g.rating,
        g.ratings_count,
        d.rating_overall,
        d.reviews_count,
        d.reviews_text_count,

        --categorisation

        -- engagement
        d.achievements_count,

        --media and community
        d.background_image,
        d.reddit_name,
        d.reddit_url,

        -- relationships (id arrays)
        d.platform_id,
        d.esrb_rating_id,
        d.developer_id,
        d.publisher_id,
        d.genre_id,
        d.tag_id,

        -- metadata
        d.updated_on_rawg,
        g.updated_at

    from games g
    left join
        details d using (game_id)
)

select * from final
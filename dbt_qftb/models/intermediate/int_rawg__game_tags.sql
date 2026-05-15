with

game_tags_unnested as (
    select
        game_id,
        description_raw,
        background_image,
        ratings_count,
        reviews_count,
        reviews_text_count,
        achievements_count,
        reddit_url,

        unnest(tag_id) as tag_id

    from {{ ref("stg_rawg__game_details") }}
),

game_releases as (
    select
        game_id,
        game_name,
        game_date_released

    from {{ ref("stg_rawg__games") }}
),

tags as (
    select * from {{ ref("stg_rawg__tags") }}
),

final as (
    select
        gr.game_id,
        gr.game_name,
        gr.game_date_released,
        g.description_raw,
        g.background_image,
        g.ratings_count,
        g.reviews_count,
        g.reviews_text_count,
        g.achievements_count,
        g.reddit_url,
        t.tag_id,
        t.tag_name

    from game_tags_unnested as g

    left join
        tags as t
        using (tag_id)

    left join
        game_releases as gr
        using (game_id)

    where
        t.tag_id is not null

    order by
        gr.game_date_released desc,
        gr.game_name asc,
        t.tag_name asc
)

select * from final

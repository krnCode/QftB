with game_tags_unnested as (
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
        name,
        released
    from {{ ref("stg_rawg__games") }}
),

tags as (
    select * from {{ ref("stg_rawg__tags") }}
),

final as (
    select
        gr.game_id,
        gr.name,
        gr.released,
        g.description_raw,
        g.background_image,
        g.ratings_count,
        g.reviews_count,
        g.reviews_text_count,
        g.achievements_count,
        g.reddit_url,
        t.tag_id,
        t.tag_name
    from game_tags_unnested g
    left join 
        tags t using (tag_id)
    left join 
        game_releases gr using (game_id)
    where 
        t.tag_id is not null
    order by 
        gr.released desc, 
        gr.name asc,
        t.tag_name asc
)

select * from final
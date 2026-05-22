with

games as (
    select
        game_id,
        game_name,
        game_date_released

    from {{ ref("stg_rawg__games") }}
),

game_details as (
    select
        game_id,

        unnest(platform_id) as platform_id

    from {{ ref("stg_rawg__game_details") }}
),

platforms as (
    select
        parent_platform_id,
        platform_id,
        parent_platform_name,
        platform_name

    from {{ ref("int_rawg__parent_platform_unnested") }}
),

joined_games_and_platforms as (
    select
        -- ids
        gd.game_id,
        p.parent_platform_id,
        p.platform_id,

        -- strings
        g.game_name,
        p.parent_platform_name,
        p.platform_name,

        -- dates
        g.game_date_released

    from
        game_details as gd

    left join games as g
        using (game_id)

    left join
        platforms as p
        using (platform_id)
)

select * from joined_games_and_platforms

with

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
        p.parent_platform_name,
        p.platform_name

    from
        game_details as gd

    left join
        platforms as p
        using (platform_id)
)

select * from joined_games_and_platforms

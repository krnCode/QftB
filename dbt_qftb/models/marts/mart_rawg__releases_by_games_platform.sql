with games as (
    select 
        game_id,
        name as game_name,
        released as game_released,
        unnest(platform_id) as platform_id
    from 
        {{ source('rawg', 'rawg_games') }}
),

platforms as (
    select * from {{ ref('int_rawg__parent_platform_unnested') }}
),

games_platforms as (
    select
        games.game_id,
        games.game_name,
        games.game_released,
        platforms.platform_name,
        platforms.parent_platform_name,
        platforms.platform_image
    from 
        games
    left join platforms
        using (platform_id)
)

select * from games_platforms
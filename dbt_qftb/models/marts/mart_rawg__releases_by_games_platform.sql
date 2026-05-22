with

games_by_platforms as (
    select * from {{ ref('int_rawg__game_platforms_unnested') }}
),

group_by_date as (
    select
        platform_name,
        parent_platform_name,

        --- agregations
        COUNT(platform_name) as platform_releases_count,

        --- numerics
        EXTRACT(year from game_date_released) as extracted_year,
        EXTRACT(month from game_date_released) as extacted_month,

        --- strings
        TO_CHAR(game_date_released, 'YYYY-MM') as month_year

    from games_by_platforms

    group by
        EXTRACT(year from game_date_released),
        EXTRACT(month from game_date_released),
        TO_CHAR(game_date_released, 'YYYY-MM'),
        platform_name,
        parent_platform_name
),

ranked_platforms as (
    select
        *,
        DENSE_RANK() over (
            partition by month_year order by platform_releases_count desc
        ) as rank

    from group_by_date
),

classify_platforms_by_rank as (
    select
        *,
        case
            when rank = 1 then 'Top 1'
            when rank = 2 then 'Top 2'
            when rank = 3 then 'Top 3'
            when rank = 4 then 'Top 4'
            when rank = 5 then 'Top 5'
            else 'Other Platforms'
        end as rank_category
    from ranked_platforms
),

ordered_platforms as (
    select *

    from
        classify_platforms_by_rank

    order by
        month_year desc,
        rank asc,
        platform_name asc
)

select * from ordered_platforms

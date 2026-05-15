with

games as (
    select * from {{ ref("stg_rawg__games") }}
),

group_by_date as (
    select
        -- aggregations
        COUNT(g.game_id) as game_count,

        -- strings
        TO_CHAR(g.game_date_released, 'YYYY-MM') as month_year,

        -- numerics
        EXTRACT(year from g.game_date_released) as extracted_year,
        EXTRACT(month from g.game_date_released) as extracted_month

    from games as g

    where g.game_date_released is not null

    group by
        EXTRACT(year from g.game_date_released),
        EXTRACT(month from g.game_date_released),
        TO_CHAR(g.game_date_released, 'YYYY-MM')
),

ordered as (
    select *

    from group_by_date

    order by month_year desc
)

select * from ordered

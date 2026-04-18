with games as (
    select * from {{ ref("stg_rawg__games") }}
),

group_by_date as (
    select
        -- aggregations
        COUNT(g.game_id) as game_count,

        -- dates
        EXTRACT(YEAR FROM g.released) as year,
        EXTRACT(MONTH FROM g.released) as month,

        -- concatenated date
        TO_CHAR(g.released, 'YYYY-MM') as month_year
    from games g
    where g.released is not null
    group by 
        EXTRACT(YEAR FROM g.released), 
        EXTRACT(MONTH FROM g.released),
        TO_CHAR(g.released, 'YYYY-MM')
)

select *
from group_by_date
order by month_year desc
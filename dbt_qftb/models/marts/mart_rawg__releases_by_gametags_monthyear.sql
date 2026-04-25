with game_tags as (
    select * from {{ ref("int_rawg__game_tags") }}
),

group_by_date as (
    select
        --- agregations
        DISTINCT COUNT(tag_name) as tag_count,

        --- dates
        EXTRACT(YEAR FROM released) as year,
        EXTRACT(MONTH FROM released) as month,
        
        --- concatenated date
        TO_CHAR(released, 'YYYY-MM') as month_year,

        --- identifiers
        tag_name as game_tag

    from game_tags
    group by
        EXTRACT(YEAR FROM released),
        EXTRACT(MONTH FROM released),
        TO_CHAR(released, 'YYYY-MM'),
        game_tag
), 

ranked_tags as (
    select
        *,
        DENSE_RANK() OVER (PARTITION BY month_year ORDER BY tag_count DESC) as rank
    from group_by_date
)

select 
    * 
from 
    ranked_tags
order by 
    month_year desc,
    rank asc,
    game_tag asc
with

game_tags as (
    select * from {{ ref("int_rawg__game_tags") }}
),

group_by_date as (
    select
        tag_name_capitalized as game_tag,

        --- agregations
        COUNT(tag_name_capitalized) as tag_count,

        --- numerics
        EXTRACT(year from game_date_released) as extracted_year,
        EXTRACT(month from game_date_released) as extacted_month,

        --- strings
        TO_CHAR(game_date_released, 'YYYY-MM') as month_year

    from game_tags

    group by
        EXTRACT(year from game_date_released),
        EXTRACT(month from game_date_released),
        TO_CHAR(game_date_released, 'YYYY-MM'),
        game_tag
),

ranked_tags as (
    select
        *,
        DENSE_RANK() over (
            partition by month_year order by tag_count desc
        ) as rank

    from group_by_date
),

classify_tags_by_rank as (
    select
        *,
        case
            when rank = 1 then 'Top 1'
            when rank = 2 then 'Top 2'
            when rank = 3 then 'Top 3'
            when rank = 4 then 'Top 4'
            when rank = 5 then 'Top 5'
            when rank = 6 then 'Top 6'
            when rank = 7 then 'Top 7'
            when rank = 8 then 'Top 8'
            when rank = 9 then 'Top 9'
            when rank = 10 then 'Top 10'
            else 'Other Tags'
        end as rank_category
    from ranked_tags
),

ordered_tags as (
    select *

    from
        classify_tags_by_rank

    order by
        month_year desc,
        rank asc,
        game_tag asc
)

select * from ordered_tags

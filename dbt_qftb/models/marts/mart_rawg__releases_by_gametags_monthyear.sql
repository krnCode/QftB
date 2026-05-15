with

game_tags as (
    select * from {{ ref("int_rawg__game_tags") }}
),

group_by_date as (
    select
        tag_name as game_tag,

        --- agregations
        COUNT(tag_name) as tag_count,

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

ordered_tags as (
    select *

    from
        ranked_tags

    order by
        month_year desc,
        rank asc,
        game_tag asc
)

select * from ordered_tags

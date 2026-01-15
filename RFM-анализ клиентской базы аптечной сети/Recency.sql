with max_date as (
    select max(datetime::date) as max_day from bonuscheques
), min_recency as (
    select 
        card, 
        round(min(md.max_day - datetime::date),-1) as m_rec
    from bonuscheques
    cross join max_date md
    group by card
), recency_data as (
    select m_rec, count(*) as cnt_sum
    from min_recency
    group by m_rec
), recency_bounds as (
    with range_stats as (
        select 
            max(m_rec) as max_recency,
            min(m_rec) as min_recency,
            (max(m_rec) - min(m_rec)) / 3 as range_size
        from min_recency
    ), ranges as (
        select 
            min_recency as range1_start,
            min_recency + range_size as range1_end,
            min_recency + range_size as range2_start,
            min_recency + range_size * 2 as range2_end
        from range_stats
    ), peaks as (
        select * from (
            select 1 as range_num, m_rec, cnt_sum
            from recency_data 
            where m_rec between (select range1_start from ranges) and (select range1_end from ranges)
            order by cnt_sum desc, m_rec desc limit 1
        ) as p1 union all
        select * from (
            select 2 as range_num, m_rec, cnt_sum
            from recency_data 
            where m_rec between (select range2_start from ranges) and (select range2_end from ranges)
            order by cnt_sum desc, m_rec desc limit 1
        ) as p2
    )
    select 
        coalesce((select m_rec from peaks where range_num = 1), (select range1_end from ranges)) as bound_1_2,
        coalesce((select m_rec from peaks where range_num = 2), (select range2_end from ranges)) as bound_2_3
    from peaks limit 1
)
select 
    rd.m_rec,
    rd.cnt_sum,
    case 
        when rd.m_rec <= (select bound_1_2 from recency_bounds) then 'Группа_1'
        when rd.m_rec <= (select bound_2_3 from recency_bounds) then 'Группа_2'
        else 'Группа_3'
    end as color_group,
    (select bound_1_2 from recency_bounds) as bound_1_2,
    (select bound_2_3 from recency_bounds) as bound_2_3
from recency_data rd
order by rd.m_rec
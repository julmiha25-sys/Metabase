with a as (
    select entry_at, user_id  from userentry
    where true
    [[and entry_at between {{date1}} and {{date2}} ]]
    ),
b as 
    (select to_char(a.entry_at, 'YYYY-MM') as date, count(distinct user_id) as cnt from a
    group by date)
select 'MAU_mediana' as Название, percentile_cont(0.5) within group (order by b.cnt) as Значение from b
union all
select 'MAU_avg' as Название,  avg(b.cnt) as Значение from b

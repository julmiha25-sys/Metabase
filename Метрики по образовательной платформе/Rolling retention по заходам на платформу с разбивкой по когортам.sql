with a as (
    select * from users
    where true
    [[and date_joined between {{date1}} and {{date2}}]]
    ),
b as 
   (select userentry.user_id, to_char(a.date_joined, 'YYYY-MM') as cohort, extract(day from (userentry.entry_at - a.date_joined))  as day from userentry 
    left join a on userentry.user_id=a.id  
    where extract(day from (userentry.entry_at - a.date_joined)) >= 0 
    order by user_id, day
    )
select cohort,
round(count(distinct case when day>=0 then user_id end)*100.0/count(distinct case when day>=0 then user_id end),1) as "0",
round(count(distinct case when day>=1 then user_id end)*100.0/count(distinct case when day>=0 then user_id end),1) as "1",
round(count(distinct case when day>=3 then user_id end)*100.0/count(distinct case when day>=0 then user_id end),1) as "3",
round(count(distinct case when day>=7 then user_id end)*100.0/count(distinct case when day>=0 then user_id end),1) as "7",
round(count(distinct case when day>=14 then user_id end)*100.0/count(distinct case when day>=0 then user_id end),1) as "14",
round(count(distinct case when day>=30 then user_id end)*100.0/count(distinct case when day>=0 then user_id end),1) as "30",
round(count(distinct case when day>=60 then user_id end)*100.0/count(distinct case when day>=0 then user_id end),1) as "60",
round(count(distinct case when day>=90 then user_id end)*100.0/count(distinct case when day>=0 then user_id end),1) as "90"
from b
group by cohort
order by cohort
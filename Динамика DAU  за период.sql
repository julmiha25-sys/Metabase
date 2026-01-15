select count(distinct user_id) as DAU, entry_at::date as date from userentry
where true
[[and entry_at between {{date1}} and {{date2}}]]
group by date
order by date
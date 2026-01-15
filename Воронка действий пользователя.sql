select name as Название_операции, cnt as Количество from (
select 1 as col, 'Заход на сайт' as name, count(*) as cnt from stat
where event like 'enter_site'
[[and {{date1}}]]
group by event
union 
select 2 as col, 'Открытие страницы' as name, count(*) as cnt from stat
where event like 'open_page'
[[and {{date1}}]]
group by event 
union
select 3 as col, 'Консультация' as name, count(*) as cnt from stat
where event like 'request_consultation'
[[and {{date1}}]]
group by event 
union
select 4 as col, 'Демо' as name, count(*) as cnt  from stat
where event like 'request_demo'
[[and {{date1}}]]
group by event 
union
select 5 as col, 'Оплата' as name, count(*) as cnt from stat
where event like 'go_to_payment'
[[and {{date1}}]]
group by event 
union
select 6 as col, 'Результаты теста' as name, count(*) as cnt from stat
where event like 'get_test_results'
[[and {{date1}}]]
group by event 
order by col
) as t

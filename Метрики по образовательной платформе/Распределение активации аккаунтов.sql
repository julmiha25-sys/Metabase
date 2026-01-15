select (case
        when is_active=0 then 'Не подтвержден'
        else 'Подтвержден'
        end) as Активность, 
        count(*) as Количество from users
where true
[[and date_joined between {{date1}} and {{date2}} ]]
and {{act}}
group by is_active
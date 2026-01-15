select company.name as Название_компании, count(*) as Количество_регистраций from users 
join company  on company.id=users.company_id
where true
[[and {{company1}}]]
[[and date_joined between {{date1}} and {{date2}}]]
group by company.name
order by company.name
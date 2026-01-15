with userentry_data as (
    select 
        extract(month from entry_at) as month,
        extract(quarter from entry_at) as quarter,
        user_id
    from userentry
    where extract(year from entry_at) = {{date1}}
),
attempts_data as (
    select 
        extract(month from created_at) as month,
        extract(quarter from created_at) as quarter
    from codesubmit
    where extract(year from created_at) = {{date1}}
    union all
    select 
        extract(month from created_at) as month,
        extract(quarter from created_at) as quarter
    from coderun
    where extract(year from created_at) = {{date1}}
),
successful_attempts_data as (
    select 
        extract(month from created_at) as month,
        extract(quarter from created_at) as quarter
    from codesubmit
    where extract(year from created_at) = {{date1}} and is_false = 0
),
solved_tasks_data as (
    select 
        distinct problem_id,
        extract(month from created_at) as month,
        extract(quarter from created_at) as quarter,
        user_id,
        problem_id
    from codesubmit
    where extract(year from created_at) = {{date1}} and is_false = 0
),
aggregated_userentry as (
    select 
        month,
        quarter,
        count(*) as total_entries,
        count(distinct user_id) as unique_users
    from userentry_data
    group by month, quarter
),
aggregated_attempts as (
    select 
        month,
        quarter,
        count(*) as attempts
    from attempts_data
    group by month, quarter
),
aggregated_successful_attempts as (
    select 
        month,
        quarter,
        count(*) as successful_attempts
    from successful_attempts_data
    group by month, quarter
),
aggregated_solved_tasks as (
    select 
        month,
        quarter,
        count(*) as solved_users
    from solved_tasks_data
    group by month, quarter
)
select Название, "Январь", "Февраль", "Март", "Q1", "Апрель", "Май", "Июнь", "Q2", "Июль", "Август", "Сентябрь", "Q3", "Октябрь", "Ноябрь", "Декабрь", "Q4"
from (
select
    'Всего заходов на платформу' as Название,
    1 as order_col,
    sum(case when month = 1 then total_entries else 0 end) as "Январь",
    sum(case when month = 2 then total_entries else 0 end) as "Февраль",
    sum(case when month = 3 then total_entries else 0 end) as "Март",
    sum(case when quarter = 1 then total_entries else 0 end) as "Q1",
    sum(case when month = 4 then total_entries else 0 end) as "Апрель",
    sum(case when month = 5 then total_entries else 0 end) as "Май",
    sum(case when month = 6 then total_entries else 0 end) as "Июнь",
    sum(case when quarter = 2 then total_entries else 0 end) as "Q2",
    sum(case when month = 7 then total_entries else 0 end) as "Июль",
    sum(case when month = 8 then total_entries else 0 end) as "Август",
    sum(case when month = 9 then total_entries else 0 end) as "Сентябрь",
    sum(case when quarter = 3 then total_entries else 0 end) as "Q3",
    sum(case when month = 10 then total_entries else 0 end) as "Октябрь",
    sum(case when month = 11 then total_entries else 0 end) as "Ноябрь",
    sum(case when month = 12 then total_entries else 0 end) as "Декабрь",
    sum(case when quarter = 4 then total_entries else 0 end) as "Q4"
from aggregated_userentry
union all
select 
    'Уникальных' as Название,
    2 as order_col,
    sum(case when month = 1 then unique_users else 0 end) as "Январь",
    sum(case when month = 2 then unique_users else 0 end) as "Февраль",
    sum(case when month = 3 then unique_users else 0 end) as "Март",
    sum(case when quarter = 1 then unique_users else 0 end) as "Q1",
    sum(case when month = 4 then unique_users else 0 end) as "Апрель",
    sum(case when month = 5 then unique_users else 0 end) as "Май",
    sum(case when month = 6 then unique_users else 0 end) as "Июнь",
    sum(case when quarter = 2 then unique_users else 0 end) as "Q2",
    sum(case when month = 7 then unique_users else 0 end) as "Июль",
    sum(case when month = 8 then unique_users else 0 end) as "Август",
    sum(case when month = 9 then unique_users else 0 end) as "Сентябрь",
    sum(case when quarter = 3 then unique_users else 0 end) as "Q3",
    sum(case when month = 10 then unique_users else 0 end) as "Октябрь",
    sum(case when month = 11 then unique_users else 0 end) as "Ноябрь",
    sum(case when month = 12 then unique_users else 0 end) as "Декабрь",
    sum(case when quarter = 4 then unique_users else 0 end) as "Q4"
from aggregated_userentry
union all
select 
    'Попыток решения задач' as Название,
    3 as order_col,
    sum(case when month = 1 then attempts else 0 end) as "Январь",
    sum(case when month = 2 then attempts else 0 end) as "Февраль",
    sum(case when month = 3 then attempts else 0 end) as "Март",
    sum(case when quarter = 1 then attempts else 0 end) as "Q1",
    sum(case when month = 4 then attempts else 0 end) as "Апрель",
    sum(case when month = 5 then attempts else 0 end) as "Май",
    sum(case when month = 6 then attempts else 0 end) as "Июнь",
    sum(case when quarter = 2 then attempts else 0 end) as "Q2",
    sum(case when month = 7 then attempts else 0 end) as "Июль",
    sum(case when month = 8 then attempts else 0 end) as "Август",
    sum(case when month = 9 then attempts else 0 end) as "Сентябрь",
    sum(case when quarter = 3 then attempts else 0 end) as "Q3",
    sum(case when month = 10 then attempts else 0 end) as "Октябрь",
    sum(case when month = 11 then attempts else 0 end) as "Ноябрь",
    sum(case when month = 12 then attempts else 0 end) as "Декабрь",
    sum(case when quarter = 4 then attempts else 0 end) as "Q4"
from aggregated_attempts
union all
select 
    'Успешных попыток' as Название,
    4 as order_col,
    sum(case when month = 1 then successful_attempts else 0 end) as "Январь",
    sum(case when month = 2 then successful_attempts else 0 end) as "Февраль",
    sum(case when month = 3 then successful_attempts else 0 end) as "Март",
    sum(case when quarter = 1 then successful_attempts else 0 end) as "Q1",
    sum(case when month = 4 then successful_attempts else 0 end) as "Апрель",
    sum(case when month = 5 then successful_attempts else 0 end) as "Май",
    sum(case when month = 6 then successful_attempts else 0 end) as "Июнь",
    sum(case when quarter = 2 then successful_attempts else 0 end) as "Q2",
    sum(case when month = 7 then successful_attempts else 0 end) as "Июль",
    sum(case when month = 8 then successful_attempts else 0 end) as "Август",
    sum(case when month = 9 then successful_attempts else 0 end) as "Сентябрь",
    sum(case when quarter = 3 then successful_attempts else 0 end) as "Q3",
    sum(case when month = 10 then successful_attempts else 0 end) as "Октябрь",
    sum(case when month = 11 then successful_attempts else 0 end) as "Ноябрь",
    sum(case when month = 12 then successful_attempts else 0 end) as "Декабрь",
    sum(case when quarter = 4 then successful_attempts else 0 end) as "Q4"
from aggregated_successful_attempts
union all
select 
    'Успешно решеных задач' as Название,
    5 as order_col,
    sum(case when month = 1 then solved_users else 0 end) as "Январь",
    sum(case when month = 2 then solved_users else 0 end) as "Февраль",
    sum(case when month = 3 then solved_users else 0 end) as "Март",
    sum(case when quarter = 1 then solved_users else 0 end) as "Q1",
    sum(case when month = 4 then solved_users else 0 end) as "Апрель",
    sum(case when month = 5 then solved_users else 0 end) as "Май",
    sum(case when month = 6 then solved_users else 0 end) as "Июнь",
    sum(case when quarter = 2 then solved_users else 0 end) as "Q2",
    sum(case when month = 7 then solved_users else 0 end) as "Июль",
    sum(case when month = 8 then solved_users else 0 end) as "Август",
    sum(case when month = 9 then solved_users else 0 end) as "Сентябрь",
    sum(case when quarter = 3 then solved_users else 0 end) as "Q3",
    sum(case when month = 10 then solved_users else 0 end) as "Октябрь",
    sum(case when month = 11 then solved_users else 0 end) as "Ноябрь",
    sum(case when month = 12 then solved_users else 0 end) as "Декабрь",
    sum(case when quarter = 4 then solved_users else 0 end) as "Q4"
from aggregated_solved_tasks
) as sum
order by order_col
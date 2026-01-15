select user_id  from codesubmit c
     where true
     [[and created_at between {{date1}} and {{date2}}]]
     ) as user_attempts
     
    union all
    
    select 'Успешно_решены_задачи' as name, count(distinct user_id) as Количество  from codesubmit c
    where is_false=0 
    [[and created_at between {{date1}} and {{date2}}]]
    
    union all
    
    select 'Пополнен_кошелек' as name, count(distinct user_id) as Количество from transaction
    where type_id=2 
    [[and created_at between {{date1}} and {{date2}}]]
) as t 

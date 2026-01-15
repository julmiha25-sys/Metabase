with a as (
        select user_id, count(*) as cnt   from transaction 
        left join transactiontype on transaction.type_id=transactiontype.type
        where transactiontype.type=1 or transactiontype.type between 23 and 28
        [[and created_at between {{date1}} and {{date2}}]]  
        group by user_id
        )
select 'Купили один раз' as name, sum(case when a.cnt=1 then 1 else 0 end) as Количество from a
union
select 'Купили повторно' as name, sum(case when a.cnt>1 then 1 else 0 end) as Количество from a
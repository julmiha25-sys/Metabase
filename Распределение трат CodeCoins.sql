select description as Название_операции, count(*) as Количество_операций  from transaction 
    join transactiontype on transaction.type_id=transactiontype.type
    where transaction.type_id = 1 or transaction.type_id between 23 and 28
    [[and created_at between {{date1}} and {{date2}}]]
    group by description
    order by Количество_операций  desc
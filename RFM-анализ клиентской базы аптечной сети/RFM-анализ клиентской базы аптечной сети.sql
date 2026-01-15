--Определение максимальной даты выборки
with max_date as (
    select max(datetime::date) as max_day from bonuscheques
--Определение минимальной давности покупки  
), min_recency as (
    select 
        card, 
        round(min(md.max_day - datetime::date),-1) as m_rec,
        count(*) as cnt,
        round(sum(summ),-2) as sum_c
    from bonuscheques
    cross join max_date md
    group by card
), 
--Определение групп по recency
recency_data as (
    select m_rec, count(*) as cnt_sum
    from min_recency
    group by m_rec
--Распределение на группы с использованием алгоритм адаптивной сегментации по локальным максимумам в диапазонах распределения
--Весь диапазон давности делится на 3 равные части, в каждом диапазоне находится значение с максимальной плотностью клиентов
), recency_bounds as (
    with range_stats as (
        select 
            max(m_rec) as max_recency,
            min(m_rec) as min_recency,
            (max(m_rec) - min(m_rec)) / 3 as range_size
        from min_recency
    ), ranges as (
        select 
            min_recency as range1_start,
            min_recency + range_size as range1_end,
            min_recency + range_size as range2_start,
            min_recency + range_size * 2 as range2_end
        from range_stats
    ), peaks as (
        select * from (
            select 1 as range_num, m_rec, cnt_sum
            from recency_data 
            where m_rec between (select range1_start from ranges) and (select range1_end from ranges)
            order by cnt_sum desc, m_rec desc limit 1
        ) as p1 union all
        select * from (
            select 2 as range_num, m_rec, cnt_sum
            from recency_data 
            where m_rec between (select range2_start from ranges) and (select range2_end from ranges)
            order by cnt_sum desc, m_rec desc limit 1
        ) as p2
    )
    select 
        coalesce((select m_rec from peaks where range_num = 1), (select range1_end from ranges)) as bound_1_2,
        coalesce((select m_rec from peaks where range_num = 2), (select range2_end from ranges)) as bound_2_3
    from peaks limit 1
), 
--Определение количества покупок frequency для каждого клиента, округление не проводилось из-за небольшого диапазона частот  
frequency_data as (
    select cnt, count(*) as cnt_f
    from min_recency
    group by cnt
--Распределение на группы с помощью алгоритма сегментации по математическим 2-м пикам распределения (пик после впадины)
--Если данные попадают под унимодальное распределение, то деление диапазона осуществляется на квантили по 50% от первого пика
), frequency_peaks as (
    with peaks_data as (
        select cnt, cnt_f,
            case when cnt_f > coalesce(lag(cnt_f) over (order by cnt), 0)
                  and cnt_f > coalesce(lead(cnt_f) over (order by cnt), 0)
                 then 'Пик' end as is_peak,
            case when cnt_f < coalesce(lag(cnt_f) over (order by cnt), cnt_f + 1)
                  and cnt_f < coalesce(lead(cnt_f) over (order by cnt), cnt_f + 1)
                 then 'Впадина' end as is_trough
        from frequency_data
    ), first_trough as (
        select min(case when is_trough = 'Впадина' then cnt end) as first_trough
        from peaks_data
    ), numbered_peaks as (
        select cnt, cnt_f, row_number() over (order by cnt_f desc) as peak_number
        from peaks_data, first_trough
        where is_peak = 'Пик' and cnt > first_trough
    )
    select 
        max(case when peak_number = 1 then cnt end) as peak1_val,
        max(case when peak_number = 2 then cnt end) as peak2_val
    from numbered_peaks
    where peak_number <= 2
), frequency_bounds as (
    select 
        coalesce(peak1_val, 0) as peak1_val,
        coalesce(peak2_val, 0) as peak2_val,
        case when peak1_val >= peak2_val then peak1_val else peak2_val end as high_peak,
        case when peak1_val >= peak2_val then peak2_val else peak1_val end as low_peak
    from frequency_peaks
), 
--Определение суммы покупок monetary для каждого клиента
monetary_data as (
    select sum_c, count(*) as cnt_mon
    from min_recency
    group by sum_c
--Распределение на группы с помощью алгоритма сегментации по математическим 2-м пикам распределения (пик после впадины)
--Если данные попадают под унимодальное распределение, то деление диапазона осуществляется на квантили по 50% от первого пика
), monetary_peaks as (
    with peaks_data as (
        select sum_c, cnt_mon,
            case when cnt_mon > coalesce(lag(cnt_mon) over (order by sum_c), 0)
                  and cnt_mon > coalesce(lead(cnt_mon) over (order by sum_c), 0)
                 then 'Пик' end as is_peak,
            case when cnt_mon < coalesce(lag(cnt_mon) over (order by sum_c), sum_c + 1)
                  and cnt_mon < coalesce(lead(cnt_mon) over (order by sum_c), sum_c + 1)
                 then 'Впадина' end as is_trough
        from monetary_data
    ), first_trough as (
        select min(case when is_trough = 'Впадина' then sum_c end) as first_trough
        from peaks_data
    ), numbered_peaks as (
        select sum_c, cnt_mon, row_number() over (order by cnt_mon desc) as peak_number
        from peaks_data, first_trough
        where is_peak = 'Пик' and sum_c > first_trough
    )
    select 
        max(case when peak_number = 1 then sum_c end) as peak1_val,
        max(case when peak_number = 2 then sum_c end) as peak2_val
    from numbered_peaks
    where peak_number <= 2
), monetary_bounds as (
    select 
        coalesce(peak1_val, 0) as peak1_val,
        coalesce(peak2_val, 0) as peak2_val,
        case when peak1_val >= peak2_val then peak1_val else peak2_val end as high_peak,
        case when peak1_val >= peak2_val then peak2_val else peak1_val end as low_peak
    from monetary_peaks
)
--Вывод данных
select 
    card as Номер_карты, 
    m_rec as Давность_покупки, 
    cnt as Частота_покупки,
    sum_c as Сумма_покупок,
    case 
        when m_rec <= (select bound_1_2 from recency_bounds) then '1'
        when m_rec <= (select bound_2_3 from recency_bounds) then '2'
        else '3'
    end as "R",
    case 
        when cnt >= (select high_peak from frequency_bounds) then '1'
        when (select low_peak from frequency_bounds) > 0 
             and cnt >= (select low_peak from frequency_bounds) then '2'
        when (select low_peak from frequency_bounds) = 0 
             and cnt >= (select high_peak from frequency_bounds) / 2 then '2'
        else '3'
    end as "F",
    case 
        when sum_c >= (select high_peak from monetary_bounds) then '1'
        when (select low_peak from monetary_bounds) > 0 
             and sum_c >= (select low_peak from monetary_bounds) then '2'
        when (select low_peak from monetary_bounds) = 0 
             and sum_c >= (select high_peak from monetary_bounds) / 2 then '2'
        else '3'
    end as "M"
from min_recency
order by card, "R", "F", "M"
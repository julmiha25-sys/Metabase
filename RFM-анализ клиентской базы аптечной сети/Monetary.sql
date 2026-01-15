with min_recency as (
    select 
        card, 
        round(sum(summ),-2) as sum_c
    from bonuscheques
    group by card
), monetary_data as (
    select sum_c, count(*) as cnt_mon
    from min_recency
    group by sum_c
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
select 
    md.sum_c,
    md.cnt_mon,
    case 
        when md.sum_c >= (select high_peak from monetary_bounds) then 'Группа_1'
        when (select low_peak from monetary_bounds) > 0 
             and md.sum_c >= (select low_peak from monetary_bounds) then 'Группа_2'
        when (select low_peak from monetary_bounds) = 0 
             and md.sum_c >= (select high_peak from monetary_bounds) / 2 then 'Группа_2'
        else 'Группа_3'
    end as color_group,
    (select high_peak from monetary_bounds) as high_peak,
    (select low_peak from monetary_bounds) as low_peak
from monetary_data md
order by md.sum_c
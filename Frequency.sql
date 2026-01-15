with min_recency as (
    select 
        card, 
        count(*) as cnt
    from bonuscheques
    group by card
), frequency_data as (
    select cnt, count(*) as cnt_f
    from min_recency
    group by cnt
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
)
select 
    fd.cnt,
    fd.cnt_f,
    case 
        when fd.cnt >= (select high_peak from frequency_bounds) then 'Группа_1'
        when (select low_peak from frequency_bounds) > 0 
             and fd.cnt >= (select low_peak from frequency_bounds) then 'Группа_2'
        when (select low_peak from frequency_bounds) = 0 
             and fd.cnt >= (select high_peak from frequency_bounds) / 2 then 'Группа_2'
        else 'Группа_3'
    end as color_group,
    (select high_peak from frequency_bounds) as high_peak,
    (select low_peak from frequency_bounds) as low_peak
from frequency_data fd
order by fd.cnt
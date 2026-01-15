with t as (
select 
    substring(url_params from 'funnel=[^&]*') as params, 
    event 
from stat
where url_params like '%funnel=%'
[[and {{date1}}]]
union all
select 
    substring(url_params from 'utm_source=[^&]*') as params, 
    event 
from stat
where url_params like '%utm_source=%'
[[and {{date1}}]]
union all 
select 
    'source=' || substring(url_params from 'source=([^&]*)') as params, 
    event 
from stat
where (url_params like 'source=%' or url_params like '%?source=%' or url_params like '%&source=%')
[[and {{date1}}]]
union all
select 
    substring(url_params from 'discount=[^&]*') as params, 
    event 
from stat
where url_params like '%discount=%'
[[and {{date1}}]]
),
cleaned_t as (
select 
    case 
        when params like '&source=%' then 'source=' || substring(params from 8)
        else params
    end as clean_params,
    event
from t
)
select clean_params as Параметр, event as Событие, count(*) as Количество
from cleaned_t
where true
[[and (
    clean_params like {{params_filter}} 
    or {{params_filter}} is null
)]]
group by clean_params, event
order by clean_params
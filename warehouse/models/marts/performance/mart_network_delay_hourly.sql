-- Network-wide delay by hour of day: the data behind the dashboard heatmap.
{% set early = var('on_time_early_minutes') * 60 %}
{% set late = var('on_time_late_minutes') * 60 %}

select
    feed,
    service_date,
    hour(observed_ts_local)                                         as hour_local,
    count(*)                                                        as observed_arrivals,
    count(distinct route_id)                                        as routes,
    round(median(delay_secs) / 60, 2)                               as median_delay_min,
    round(quantile_cont(delay_secs, 0.9) / 60, 2)                   as p90_delay_min,
    round(100 * avg(case when delay_secs between -{{ early }} and {{ late }} then 1 else 0 end), 1)
                                                                    as pct_on_time
from {{ ref('fct_stop_arrivals') }}
where schedule_type = 'timetable'
  and not is_first_stop
group by all

-- On-time performance per route and service day, for timetable-based service
-- (MRT feeder). Rapid KL runs to frequencies instead: see mart_route_headway_daily.
{% set early = var('on_time_early_minutes') * 60 %}
{% set late = var('on_time_late_minutes') * 60 %}

with arrivals as (
    select * from {{ ref('fct_stop_arrivals') }}
    where schedule_type = 'timetable'
      and not is_first_stop   -- departures from the terminal mostly measure dwell
)

select
    a.feed,
    a.service_date,
    a.route_id,
    r.route_name,
    count(*)                                                        as observed_arrivals,
    count(distinct a.trip_instance_key)                             as observed_trips,
    round(median(a.delay_secs) / 60, 2)                             as median_delay_min,
    round(quantile_cont(a.delay_secs, 0.9) / 60, 2)                 as p90_delay_min,
    round(100 * avg(case when a.delay_secs between -{{ early }} and {{ late }} then 1 else 0 end), 1)
                                                                    as pct_on_time,
    round(100 * avg(case when a.delay_secs < -{{ early }} then 1 else 0 end), 1) as pct_early,
    round(100 * avg(case when a.delay_secs > {{ late }} then 1 else 0 end), 1)   as pct_late
from arrivals a
left join {{ ref('dim_route') }} r
    on  r.feed = a.feed
    and r.route_id = a.route_id
    and a.service_date >= r.valid_from
    and (r.valid_to is null or a.service_date < r.valid_to)
group by all

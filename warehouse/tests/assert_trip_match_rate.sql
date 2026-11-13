-- If realtime trip_ids stop matching the timetable (e.g. the operator publishes
-- a new GTFS but our daily download failed), every downstream metric silently
-- shrinks. Fail loudly instead. Baseline on day one: 98.7–100%.
{{ config(severity = 'warn') }}

with latest_complete_hour as (
    select feed, max(hour_local) as hour_local
    from {{ ref('mart_feed_health_hourly') }}
    where hour_local < date_trunc('hour', {{ to_local('now()::timestamp') }})
    group by feed
)

select h.*
from {{ ref('mart_feed_health_hourly') }} h
join latest_complete_hour l using (feed, hour_local)
where h.events >= 50
  and h.pct_trip_matched < {{ var('min_pct_trip_matched') }}

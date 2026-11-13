/*
Feed health per hour: is each feed alive, moving and joinable to its timetable?

A vehicle is "frozen" in an hour when it reported at least 3 times but never
moved more than ~11 m (4-decimal lat/lon). One frozen bus at a layover is
normal; most of a feed frozen means the publisher is sending placeholders,
as KTMB does (see README).
*/
with positions as (
    select
        feed,
        vehicle_id,
        is_trip_matched,
        date_trunc('hour', event_ts_local) as hour_local,
        round(latitude, 4)                  as lat4,
        round(longitude, 4)                 as lon4
    from {{ ref('fct_vehicle_positions') }}
),

per_vehicle as (
    select
        feed,
        hour_local,
        vehicle_id,
        count(*)                          as events,
        count(distinct (lat4, lon4))      as distinct_positions,
        count(*) filter (where is_trip_matched) as matched_events
    from positions
    group by all
)

select
    feed,
    hour_local,
    sum(events)                                                         as events,
    count(*)                                                            as vehicles,
    count(*) filter (where events >= 3 and distinct_positions = 1)      as frozen_vehicles,
    round(100.0 * count(*) filter (where events >= 3 and distinct_positions = 1)
          / count(*), 1)                                                as pct_frozen,
    round(100.0 * sum(matched_events) / sum(events), 1)                 as pct_trip_matched
from per_vehicle
group by all

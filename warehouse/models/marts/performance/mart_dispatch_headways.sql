/*
Headway adherence for frequency-based service (Rapid KL): "a bus every N
minutes" has no fixed times, so reliability = how evenly buses depart.

One row per pair of consecutive departures on the same route and direction.
A trip that never reports a position looks like a gap: from a rider's view a
bus that doesn't show up IS a gap, but an ingestion outage would too, so
pipeline_health freshness alerts matter when reading these numbers.

Gaps whose earlier departure happened before we started receiving data that
day are dropped: buses that left and finished before then were never seen, so
the first gap would span hours (on day one this alone moved the median
headway ratio from 1.23 to 1.06).
*/
with coverage as (
    select feed, event_date_local as service_date, min(event_ts_local) as coverage_start
    from {{ ref('fct_vehicle_positions') }}
    group by all
),

sequenced as (
    select
        *,
        lag(dispatch_ts_local) over w as prev_dispatch_ts_local,
        lag(vehicle_id) over w        as prev_vehicle_id
    from {{ ref('int_frequency_dispatches') }}
    window w as (
        partition by feed, service_date, route_id, direction_id
        order by dispatch_ts_local
    )
)

select
    feed,
    service_date,
    route_id,
    direction_id,
    trip_id,
    vehicle_id,
    prev_vehicle_id,
    dispatch_ts_local,
    epoch(dispatch_ts_local) - epoch(prev_dispatch_ts_local)                 as actual_headway_secs,
    scheduled_headway_secs,
    round((epoch(dispatch_ts_local) - epoch(prev_dispatch_ts_local))
          / scheduled_headway_secs, 2)                                       as headway_ratio,
    epoch(dispatch_ts_local) - epoch(prev_dispatch_ts_local)
        < {{ var('bunching_ratio') }} * scheduled_headway_secs               as is_bunched,
    epoch(dispatch_ts_local) - epoch(prev_dispatch_ts_local)
        > {{ var('gap_ratio') }} * scheduled_headway_secs                    as is_gap
from sequenced s
join coverage c using (feed, service_date)
where s.prev_dispatch_ts_local is not null
  and s.prev_dispatch_ts_local >= c.coverage_start
  -- longer than 4h is a service break or ingestion outage, not a headway
  and epoch(s.dispatch_ts_local) - epoch(s.prev_dispatch_ts_local) <= 4 * 3600

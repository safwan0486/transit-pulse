/*
Headway adherence for frequency-based service (Rapid KL): "a bus every N
minutes" has no fixed times, so reliability = how evenly buses are spaced.

One row per pair of consecutive buses passing the same stop, same route and
direction. Terminal stops are excluded (layover dwell distorts headways).
*/
with arrivals as (
    select *
    from {{ ref('fct_stop_arrivals') }}
    where schedule_type = 'frequency'
      and not is_first_stop
      and not is_last_stop
),

sequenced as (
    select
        *,
        lag(observed_ts) over w  as prev_observed_ts,
        lag(vehicle_id) over w   as prev_vehicle_id
    from arrivals
    window w as (
        partition by feed, service_date, route_id, direction_id, stop_id
        order by observed_ts
    )
),

headways as (
    select
        s.*,
        epoch(s.observed_ts) - epoch(s.prev_observed_ts) as actual_headway_secs,
        f.headway_secs                                    as scheduled_headway_secs
    from sequenced s
    -- each Rapid KL trip_id is one frequency window (e.g. ..._0 = 06:00-19:30 every 30 min)
    join {{ ref('stg_gtfs__frequencies') }} f
        on f.feed = s.feed and f.snapshot_date = s.snapshot_date and f.trip_id = s.trip_id
    where s.prev_observed_ts is not null
      and s.prev_vehicle_id <> s.vehicle_id
)

select
    feed,
    service_date,
    route_id,
    direction_id,
    stop_id,
    stop_sequence,
    vehicle_id,
    prev_vehicle_id,
    observed_ts_local,
    actual_headway_secs,
    scheduled_headway_secs,
    round(actual_headway_secs / scheduled_headway_secs, 2)                 as headway_ratio,
    actual_headway_secs < {{ var('bunching_ratio') }} * scheduled_headway_secs as is_bunched,
    actual_headway_secs > {{ var('gap_ratio') }} * scheduled_headway_secs      as is_gap
from headways
-- longer than 4h is a service break or ingestion outage, not a headway
where actual_headway_secs <= 4 * 3600

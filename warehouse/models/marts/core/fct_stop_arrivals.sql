{{
    config(
        materialized = 'incremental',
        unique_key = 'stop_visit_id',
        incremental_strategy = 'delete+insert',
    )
}}
/*
One row per vehicle visit to a scheduled stop, inferred from GPS (the feeds
don't report stop arrivals themselves).

Matching rules, found by profiling the real data:
  1. A ping is "at" a stop of its trip when within stop_match_radius_m.
  2. 94% of MRT feeder trips are loops (first stop = last stop), so one ping
     can match several stop occurrences. Each ping is assigned to ONE
     occurrence: the one closest to its schedule time.
  3. Per (trip instance, stop occurrence) keep the closest ping.
  4. Timetable matches more than max_abs_delay_minutes off schedule are
     mismatches (parallel roads), not delays, and are dropped.
*/

with positions as (
    select p.*, s.snapshot_date
    from {{ ref('stg_lake__vehicle_positions') }} p
    join {{ ref('int_gtfs__snapshot_for_service_date') }} s
        on s.feed = p.feed and s.service_date = p.event_date_local
    where p.feed in ({{ "'" ~ var('performance_feeds') | join("', '") ~ "'" }})
      and p.trip_id is not null
    {% if is_incremental() %}
      -- reprocess the last day so trips spanning the cut-off are complete
      and p.event_date_local >= (select max(service_date) - 1 from {{ this }})
    {% endif %}
),

candidates as (
    select
        p.event_id,
        p.feed,
        p.snapshot_date,
        p.vehicle_id,
        p.trip_id,
        p.rt_start_date,
        p.rt_start_time,
        p.event_ts,
        p.event_ts_local,
        p.event_date_local,
        ts.route_id,
        ts.direction_id,
        ts.stop_id,
        ts.stop_sequence,
        ts.is_first_stop,
        ts.is_last_stop,
        ts.schedule_type,
        ts.arrival_secs,
        ts.first_stop_arrival_secs,
        {{ haversine_m('p.latitude', 'p.longitude', 'ts.stop_lat', 'ts.stop_lon') }} as distance_m,
        epoch(p.event_ts_local) - epoch(date_trunc('day', p.event_ts_local)) as event_secs_of_day
    from positions p
    join {{ ref('int_gtfs__trip_stops') }} ts
        on ts.feed = p.feed and ts.snapshot_date = p.snapshot_date and ts.trip_id = p.trip_id
),

scored as (
    select
        *,
        -- Timetable service: the service day is today or (after midnight) yesterday.
        case
            when schedule_type = 'timetable'
             and abs(event_secs_of_day - arrival_secs)
                 > abs(event_secs_of_day + 86400 - arrival_secs)
            then event_date_local - 1
            else event_date_local
        end as tt_service_date,
        -- Frequency service: realtime start_date/start_time = actual dispatch.
        strptime(rt_start_date, '%Y%m%d')                    as dispatch_date,
        {{ gtfs_time_to_seconds('rt_start_time') }}          as dispatch_secs
    from candidates
    where distance_m <= {{ var('stop_match_radius_m') }}
),

timed as (
    select
        *,
        case when schedule_type = 'timetable' then
            cast(tt_service_date as timestamp) + to_seconds(arrival_secs)
        end as scheduled_arrival_ts_local,
        case when schedule_type = 'frequency' and dispatch_date is not null then
            dispatch_date + to_seconds(dispatch_secs + arrival_secs - first_stop_arrival_secs)
        end as expected_arrival_ts_local
    from scored
),

deviations as (
    select
        *,
        epoch(event_ts_local) - epoch(scheduled_arrival_ts_local) as delay_secs,
        epoch(event_ts_local) - epoch(expected_arrival_ts_local)  as running_time_deviation_secs,
        case
            when schedule_type = 'timetable' then cast(tt_service_date as date)
            else cast(coalesce(dispatch_date, event_date_local) as date)
        end as service_date
    from timed
),

-- rule 2: one stop occurrence per ping
per_ping as (
    select
        *,
        row_number() over (
            partition by event_id
            order by abs(coalesce(delay_secs, running_time_deviation_secs, 0)), distance_m
        ) as ping_rank
    from deviations
    where delay_secs is null
       or abs(delay_secs) <= {{ var('max_abs_delay_minutes') }} * 60
),

keyed as (
    select
        *,
        case
            when schedule_type = 'timetable'
                then concat_ws('|', feed, trip_id, service_date)
            else concat_ws('|', feed, trip_id, rt_start_date, rt_start_time, vehicle_id)
        end as trip_instance_key
    from per_ping
    where ping_rank = 1
),

-- rule 3: closest ping per stop occurrence
visits as (
    select
        *,
        row_number() over (
            partition by trip_instance_key, stop_sequence
            order by distance_m, event_ts
        ) as visit_rank
    from keyed
)

select
    md5(trip_instance_key || '|' || stop_sequence)      as stop_visit_id,
    trip_instance_key,
    feed,
    service_date,
    snapshot_date,
    vehicle_id,
    trip_id,
    route_id,
    direction_id,
    stop_id,
    stop_sequence,
    is_first_stop,
    is_last_stop,
    schedule_type,
    event_ts                                             as observed_ts,
    event_ts_local                                       as observed_ts_local,
    round(distance_m, 1)                                 as distance_m,
    scheduled_arrival_ts_local,
    delay_secs,
    expected_arrival_ts_local,
    running_time_deviation_secs,
    event_id                                             as source_event_id
from visits
where visit_rank = 1

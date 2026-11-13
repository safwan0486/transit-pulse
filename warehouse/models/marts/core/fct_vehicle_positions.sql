{{
    config(
        materialized = 'incremental',
        unique_key = 'event_id',
        incremental_strategy = 'delete+insert',
    )
}}
-- Vehicle positions enriched with the route from the timetable
-- (the realtime route_id is unreliable for MRT feeder and missing for KTMB).
with positions as (
    select p.*, s.snapshot_date
    from {{ ref('stg_lake__vehicle_positions') }} p
    left join {{ ref('int_gtfs__snapshot_for_service_date') }} s
        on s.feed = p.feed and s.service_date = p.event_date_local
    {% if is_incremental() %}
    where p.event_ts > (select max(event_ts) - interval 2 hour from {{ this }})
    {% endif %}
)

select
    p.event_id,
    p.feed,
    p.vehicle_id,
    p.license_plate,
    p.trip_id,
    t.route_id,
    p.rt_route_id,
    t.direction_id,
    t.trip_id is not null           as is_trip_matched,
    p.latitude,
    p.longitude,
    p.bearing,
    p.speed_kmh,
    p.event_ts,
    p.event_ts_local,
    p.event_date_local,
    p.ingested_at
from positions p
left join {{ ref('stg_gtfs__trips') }} t
    on t.feed = p.feed and t.snapshot_date = p.snapshot_date and t.trip_id = p.trip_id

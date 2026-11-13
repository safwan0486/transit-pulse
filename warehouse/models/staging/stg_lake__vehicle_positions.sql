select
    event_id,
    feed,
    vehicle_id,
    license_plate,
    trip_id,
    -- The realtime route_id is unreliable (MRT feeder sends the public route
    -- number, KTMB sends nothing). Kept for display; joins use the trip's route.
    route_id                                   as rt_route_id,
    start_date                                 as rt_start_date,
    start_time                                 as rt_start_time,
    latitude,
    longitude,
    bearing,
    speed_kmh,
    speed_reported_raw,
    event_ts,
    {{ to_local('event_ts') }}                 as event_ts_local,
    cast(event_date as date)                   as event_date_local,
    ingested_at,
    processed_at
from {{ source('lake', 'vehicle_positions') }}

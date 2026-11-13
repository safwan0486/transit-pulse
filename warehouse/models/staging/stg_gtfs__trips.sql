select
    feed,
    cast(dt as date)                           as snapshot_date,
    trip_id,
    route_id,
    service_id,
    try_cast(direction_id as integer)          as direction_id,
    nullif(trip_headsign, '')                  as trip_headsign,
    nullif(shape_id, '')                       as shape_id
from {{ source('lake', 'gtfs_trips') }}

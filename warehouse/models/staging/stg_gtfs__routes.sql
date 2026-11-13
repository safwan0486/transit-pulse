select
    feed,
    cast(dt as date)                           as snapshot_date,
    route_id,
    nullif(route_short_name, '')               as route_short_name,
    nullif(route_long_name, '')                as route_long_name,
    -- MRT feeder leaves route_short_name empty and puts the public number (e.g. T117)
    -- in route_long_name, so fall back to it for a display name.
    coalesce(nullif(route_short_name, ''), nullif(route_long_name, ''), route_id) as route_name,
    try_cast(route_type as integer)            as route_type,
    nullif(route_color, '')                    as route_color
from {{ source('lake', 'gtfs_routes') }}

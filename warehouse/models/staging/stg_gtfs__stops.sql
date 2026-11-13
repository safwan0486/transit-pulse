-- Note: rapid-rail-kl's stops.txt has a `geometry` column holding the literal text
-- "[object Object]" (an upstream serialisation bug), so it is deliberately not selected.
select
    feed,
    cast(dt as date)                           as snapshot_date,
    stop_id,
    stop_name,
    cast(stop_lat as double)                   as stop_lat,
    cast(stop_lon as double)                   as stop_lon
from {{ source('lake', 'gtfs_stops') }}

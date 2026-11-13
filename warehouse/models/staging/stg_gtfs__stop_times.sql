select
    feed,
    cast(dt as date)                                       as snapshot_date,
    trip_id,
    stop_id,
    cast(stop_sequence as integer)                         as stop_sequence,
    {{ gtfs_time_to_seconds('arrival_time') }}             as arrival_secs,
    {{ gtfs_time_to_seconds('departure_time') }}           as departure_secs
from {{ source('lake', 'gtfs_stop_times') }}
where arrival_time is not null

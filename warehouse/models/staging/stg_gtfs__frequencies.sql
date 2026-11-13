-- Headway-based service: "a bus every headway_secs between start and end".
select
    feed,
    cast(dt as date)                                       as snapshot_date,
    trip_id,
    {{ gtfs_time_to_seconds('start_time') }}               as start_secs,
    {{ gtfs_time_to_seconds('end_time') }}                 as end_secs,
    cast(headway_secs as integer)                          as headway_secs,
    coalesce(try_cast(exact_times as integer), 0) = 1      as is_exact_times
from {{ source('lake', 'gtfs_frequencies') }}

-- Stops as published in each feed's latest GTFS snapshot.
with latest as (
    select feed, max(snapshot_date) as snapshot_date
    from {{ ref('stg_gtfs__stops') }}
    group by feed
)

select
    s.feed,
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    s.snapshot_date
from {{ ref('stg_gtfs__stops') }} s
join latest l using (feed, snapshot_date)

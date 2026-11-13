-- Which daily GTFS snapshot was "the timetable" on a given service day:
-- the latest snapshot taken on or before that day (or the earliest one, if
-- positions pre-date every snapshot we have).
with service_dates as (
    select distinct feed, event_date_local as service_date
    from {{ ref('stg_lake__vehicle_positions') }}
    union
    -- trips that run past midnight belong to the previous service day
    select distinct feed, event_date_local - 1
    from {{ ref('stg_lake__vehicle_positions') }}
),

snapshots as (
    select distinct feed, snapshot_date from {{ ref('stg_gtfs__trips') }}
)

select
    d.feed,
    d.service_date,
    coalesce(
        max(s.snapshot_date) filter (where s.snapshot_date <= d.service_date),
        min(s.snapshot_date)
    ) as snapshot_date
from service_dates d
join snapshots s using (feed)
group by d.feed, d.service_date

/*
Routes as a Type 2 slowly changing dimension, rebuilt from the daily GTFS
snapshots in the lake (so it is reproducible: no dependence on when dbt ran).

A new version starts whenever a route's attributes change between snapshots.
Facts join on: feed, route_id and service_date in [valid_from, valid_to).
*/
with versions as (
    select
        *,
        md5(concat_ws('|',
            coalesce(route_name, ''), coalesce(route_long_name, ''),
            coalesce(cast(route_type as varchar), ''), coalesce(route_color, '')
        )) as attr_hash
    from {{ ref('stg_gtfs__routes') }}
),

flagged as (
    select
        *,
        lag(attr_hash) over (partition by feed, route_id order by snapshot_date) as prev_hash
    from versions
),

starts as (
    select * from flagged
    where prev_hash is null or prev_hash <> attr_hash
),

latest_snapshot as (
    select feed, max(snapshot_date) as latest_snapshot_date
    from {{ ref('stg_gtfs__routes') }}
    group by feed
),

last_seen as (
    select feed, route_id, max(snapshot_date) as last_seen_snapshot_date
    from {{ ref('stg_gtfs__routes') }}
    group by all
)

select
    md5(concat_ws('|', s.feed, s.route_id, s.snapshot_date))  as route_version_key,
    s.feed,
    s.route_id,
    s.route_name,
    s.route_short_name,
    s.route_long_name,
    s.route_type,
    s.route_color,
    s.snapshot_date                                           as valid_from,
    lead(s.snapshot_date) over (
        partition by s.feed, s.route_id order by s.snapshot_date
    )                                                         as valid_to,
    lead(s.snapshot_date) over (
        partition by s.feed, s.route_id order by s.snapshot_date
    ) is null
        and ls.last_seen_snapshot_date = l.latest_snapshot_date as is_current
from starts s
join latest_snapshot l using (feed)
join last_seen ls using (feed, route_id)

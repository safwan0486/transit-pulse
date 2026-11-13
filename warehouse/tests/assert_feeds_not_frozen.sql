-- Alert when most of a feed's vehicles stop moving: the publisher is likely
-- sending stale or placeholder positions. Checks each feed's latest complete
-- hour. Feeds already known to be frozen (var known_frozen_feeds) are skipped,
-- so this catches NEW breakages instead of warning forever about known ones.
{{ config(severity = 'warn') }}

with latest_complete_hour as (
    select feed, max(hour_local) as hour_local
    from {{ ref('mart_feed_health_hourly') }}
    where hour_local < date_trunc('hour', {{ to_local('now()::timestamp') }})
    group by feed
)

select h.*
from {{ ref('mart_feed_health_hourly') }} h
join latest_complete_hour l using (feed, hour_local)
where h.vehicles >= 3
  and h.pct_frozen > {{ var('max_pct_frozen') }}
  and h.feed not in ({{ "'" ~ var('known_frozen_feeds') | join("', '") ~ "'" }})

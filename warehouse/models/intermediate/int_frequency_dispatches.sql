/*
One row per observed departure of a frequency-based trip (Rapid KL).

Rapid KL's realtime feed reports each running trip's actual dispatch
(start_date + start_time). Headways between consecutive dispatches are a far
more reliable basis than headways measured at stops: GPS pings arrive about
once a minute while stops are a few hundred metres apart, so a bus is caught
near only ~34% of its stops, and every missed pass doubled the apparent gap
at that stop (median 2.0x scheduled vs 1.2x from dispatches).
*/
with positions as (
    select p.*, s.snapshot_date
    from {{ ref('stg_lake__vehicle_positions') }} p
    join {{ ref('int_gtfs__snapshot_for_service_date') }} s
        on s.feed = p.feed and s.service_date = p.event_date_local
    where p.rt_start_date is not null
      and nullif(trim(p.rt_start_time), '') is not null
)

select
    p.feed,
    p.trip_id,
    p.rt_start_date,
    p.rt_start_time,
    t.route_id,
    t.direction_id,
    f.headway_secs                                                   as scheduled_headway_secs,
    strptime(p.rt_start_date || ' ' || p.rt_start_time, '%Y%m%d %H:%M:%S') as dispatch_ts_local,
    cast(strptime(p.rt_start_date, '%Y%m%d') as date)                as service_date,
    min(p.snapshot_date)                                             as snapshot_date,
    arg_min(p.vehicle_id, p.event_ts)                                as vehicle_id,
    count(*)                                                         as position_reports
from positions p
join {{ ref('stg_gtfs__trips') }} t
    on t.feed = p.feed and t.snapshot_date = p.snapshot_date and t.trip_id = p.trip_id
join {{ ref('stg_gtfs__frequencies') }} f
    on f.feed = p.feed and f.snapshot_date = p.snapshot_date and f.trip_id = p.trip_id
group by all

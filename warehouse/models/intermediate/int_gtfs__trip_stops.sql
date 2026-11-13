-- Every scheduled stop of every trip, with stop coordinates and the trip's route.
with stop_times as (
    select * from {{ ref('stg_gtfs__stop_times') }}
),

bounds as (
    select
        feed, snapshot_date, trip_id,
        min(stop_sequence) as first_seq,
        max(stop_sequence) as last_seq,
        arg_min(arrival_secs, stop_sequence) as first_stop_arrival_secs
    from stop_times
    group by all
),

frequency_trips as (
    select distinct feed, snapshot_date, trip_id from {{ ref('stg_gtfs__frequencies') }}
)

select
    st.feed,
    st.snapshot_date,
    st.trip_id,
    t.route_id,
    t.direction_id,
    st.stop_id,
    st.stop_sequence,
    st.arrival_secs,
    b.first_stop_arrival_secs,
    st.stop_sequence = b.first_seq                      as is_first_stop,
    st.stop_sequence = b.last_seq                       as is_last_stop,
    -- "timetable": fixed time per stop; "frequency": a bus every N minutes
    case when f.trip_id is null then 'timetable' else 'frequency' end as schedule_type,
    s.stop_lat,
    s.stop_lon
from stop_times st
join {{ ref('stg_gtfs__trips') }} t
    on t.feed = st.feed and t.snapshot_date = st.snapshot_date and t.trip_id = st.trip_id
join {{ ref('stg_gtfs__stops') }} s
    on s.feed = st.feed and s.snapshot_date = st.snapshot_date and s.stop_id = st.stop_id
join bounds b
    on b.feed = st.feed and b.snapshot_date = st.snapshot_date and b.trip_id = st.trip_id
left join frequency_trips f
    on f.feed = st.feed and f.snapshot_date = st.snapshot_date and f.trip_id = st.trip_id

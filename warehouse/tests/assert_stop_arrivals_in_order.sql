-- Sanity check on the GPS→stop matching: within one trip instance, a later stop
-- should not be visited much earlier than an earlier stop. A few minutes of
-- jitter is fine (30 s polling, dwell at stops); 10+ minutes backwards means
-- the matching assigned pings to the wrong stop occurrence.
{{ config(severity = 'warn') }}

with ordered as (
    select
        trip_instance_key,
        stop_sequence,
        observed_ts,
        lag(observed_ts) over (
            partition by trip_instance_key order by stop_sequence
        ) as prev_stop_observed_ts
    from {{ ref('fct_stop_arrivals') }}
)

select *
from ordered
where observed_ts < prev_stop_observed_ts - interval 10 minute

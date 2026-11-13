/*
Daily headway reliability per route (frequency-based service), from actual
departure times (see int_frequency_dispatches for why not stop passes).

Excess Wait Time (EWT) is the standard metric for "turn up and go" service
(used by e.g. Transport for London):
    actual wait    AWT = Σh² / (2·Σh)     over actual headways h
    scheduled wait SWT = Σs² / (2·Σs)     over scheduled headways s
    EWT = AWT − SWT   → extra minutes a randomly arriving rider waits
*/
select
    h.feed,
    h.service_date,
    h.route_id,
    r.route_name,
    count(*)                                                         as observed_headways,
    round(median(h.headway_ratio), 2)                                as median_headway_ratio,
    round(100 * avg(case when h.is_bunched then 1 else 0 end), 1)    as pct_bunched,
    round(100 * avg(case when h.is_gap then 1 else 0 end), 1)        as pct_gaps,
    -- robust headline: share of departures neither bunched nor a gap
    round(100 * avg(case when not h.is_bunched and not h.is_gap then 1 else 0 end), 1)
                                                                     as pct_evenly_spaced,
    round(sum(h.actual_headway_secs ^ 2) / (2 * sum(h.actual_headway_secs)) / 60, 1)
                                                                     as avg_wait_min,
    round(sum(h.scheduled_headway_secs ^ 2) / (2 * sum(h.scheduled_headway_secs)) / 60, 1)
                                                                     as scheduled_wait_min,
    round(
        (sum(h.actual_headway_secs ^ 2) / (2 * sum(h.actual_headway_secs))
         - sum(h.scheduled_headway_secs ^ 2) / (2 * sum(h.scheduled_headway_secs))) / 60,
        1
    )                                                                as excess_wait_min
from {{ ref('mart_dispatch_headways') }} h
left join {{ ref('dim_route') }} r
    on  r.feed = h.feed
    and r.route_id = h.route_id
    and h.service_date >= r.valid_from
    and (r.valid_to is null or h.service_date < r.valid_to)
group by all

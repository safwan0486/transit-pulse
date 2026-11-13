-- SCD2 integrity: each route has at most one current version, and versions
-- of the same route never overlap. Returns offending rows (test fails if any).
with checks as (
    select
        feed,
        route_id,
        count(*) filter (where is_current) as current_versions,
        count(*) filter (
            where valid_to is not null and valid_to <= valid_from
        ) as bad_ranges
    from {{ ref('dim_route') }}
    group by all
)

select *
from checks
where current_versions > 1 or bad_ranges > 0

{#- GTFS times are "H:MM:SS" and may exceed 24:00:00 for trips after midnight. -#}
{#- Returns null for missing/blank values (e.g. MRT feeder sends no start_time). -#}
{% macro gtfs_time_to_seconds(col) -%}
    (case when nullif(trim({{ col }}), '') is not null then
        cast(split_part({{ col }}, ':', 1) as integer) * 3600
        + cast(split_part({{ col }}, ':', 2) as integer) * 60
        + cast(split_part({{ col }}, ':', 3) as integer)
    end)
{%- endmacro %}


{#- UTC timestamp → Malaysia local wall-clock timestamp. -#}
{% macro to_local(ts) -%}
    ({{ ts }} + interval '{{ var("local_utc_offset_hours") }} hours')
{%- endmacro %}


{#- Great-circle distance in metres between two lat/lon points. -#}
{% macro haversine_m(lat1, lon1, lat2, lon2) -%}
    (
        2 * 6371000 * asin(sqrt(
            pow(sin(radians({{ lat2 }} - {{ lat1 }}) / 2), 2)
            + cos(radians({{ lat1 }})) * cos(radians({{ lat2 }}))
            * pow(sin(radians({{ lon2 }} - {{ lon1 }}) / 2), 2)
        ))
    )
{%- endmacro %}


{#- Use the schema names from dbt_project.yml as-is (no "<target>_" prefix). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}

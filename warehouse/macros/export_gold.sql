{#-
Gold layer: after every `dbt build`/`dbt run`, copy the serving tables to the
lake as Parquet (s3://<bucket>/gold/<table>.parquet).

Why not let the dashboard query the DuckDB file directly? DuckDB allows one
writer at a time: the dashboard would hit lock errors whenever dbt runs, and
it would couple the serving layer to the warehouse engine. Parquet in the lake
is engine-neutral (DuckDB, Spark, BigQuery external tables can all read it).

Tables that failed to rebuild keep their previous (tested) contents, because
`dbt build` skips models downstream of a failing test, so gold never receives
untested data.
-#}
{% macro export_gold() %}
    {% if execute and flags.WHICH in ['build', 'run'] %}
        {% set gold_tables = [
            ('performance', 'mart_route_on_time_daily'),
            ('performance', 'mart_network_delay_hourly'),
            ('performance', 'mart_route_headway_daily'),
            ('monitoring', 'mart_feed_health_hourly'),
        ] %}
        {% for schema, table in gold_tables %}
            {% set relation = adapter.get_relation(database=target.database, schema=schema, identifier=table) %}
            {% if relation %}
                {% set target_path = 's3://' ~ var('lake_bucket') ~ '/gold/' ~ table ~ '.parquet' %}
                {% do run_query("copy (select * from " ~ relation ~ ") to '" ~ target_path ~ "' (format parquet)") %}
                {% do log('Exported ' ~ relation ~ ' -> ' ~ target_path, info=True) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

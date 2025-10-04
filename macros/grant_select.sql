{% macro grant_select(schema=target.schema,role=target.role) %}
    {% set sql_query %}

        grant usage on schema {{ schema }} to role {{ role }};
        grant select on all tables in schema {{ schema }} to role {{ role }};
        grant select on all views in schema {{ schema }} to role {{ role }};

    {% endset%}

    {{ log('Granting access to select all tables and views in schema '~ schema ~' to role '~ role, info=True) }}

    {% do run_query(sql_query) %}

    {{ log('Priviledges granted', info=True) }}



{% endmacro %}
{#  
    -- let's develop a macro that 
    1. queries the information schema of a database
    2. finds objects that are > 1 week old (no longer maintained)
    3. generates automated drop statements
    4. has the ability to execute those drop statements

#}

{% macro clean_stale_models(database=target.database, schema=target.schema, days=7, dry_run = True) %}

    {% set drop_sql_query %}
        select
            case
                when table_type = 'VIEW'
                then table_type
            else
                'TABLE'
            end as object_type,
            'DROP ' || object_type || ' '|| '{{ database | upper }}.' || table_schema || '.' || table_name || ';' as drop_sql_query
        from {{ database }}.information_schema.tables
        where table_schema = '{{ schema | upper }}'
        and last_altered < dateadd('day',-{{ days }}  ,current_date)    
    {% endset %}

    {{ log('Generating Drop SQL queries...\n', info=True) }}
    {% set drop_queries=run_query(drop_sql_query).columns[1].values() %}

    {% for drop_query in drop_queries %}
        
        {% if dry_run %}
            {{ log('Only generating the drop queries: ' ~ drop_query, info=True) }}
        {% else %}
            {{ log('Executing query: '~ drop_query, info=True) }}
            {% do run_query(drop_query) %}
            {{ log('Query Executed Successfully.', info=True) }}
        {% endif %}

        
    {% endfor %}
{% endmacro %}

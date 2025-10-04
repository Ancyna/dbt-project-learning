{% macro union_all_tables_prefix(database, schema, prefix) %}

    {%- set table_list=dbt_utils.get_relations_by_prefix(database=database, schema=schema, prefix=prefix) %}
    
    {% for table in table_list -%}
        {% if not loop.first -%}
            union all
        {% endif -%}

        select * from {{ table.database }}.{{ table.schema }}.{{ table.name }}
        
    {% endfor -%}
{% endmacro %}
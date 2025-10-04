{% macro template_example() %}
    {% set sql_query %}
        select true as boolean;        
    {% endset%}

    {% if execute %}
        {% set query_result=run_query(sql_query).columns[0].values()[0] %}
        {{ log('SQL executed result: '~ query_result, info=True) }}


        select {{ query_result }} as _is_real
        from a_real_table


    {% endif %}

{% endmacro %}
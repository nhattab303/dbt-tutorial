{% macro get_date_macro() -%}

{% if var('year') == '' %}
    {%- set temp= run_query('select year(getdate())') -%}
    {% if execute %}
        {% set year=temp.columns[0].values()[0] %}
    {% else %}
        {% set year=None %}
    {% endif %}
{% else %}
    {%- set year = var('year') -%}
{% endif %}

{% if var('month')=='' %}
    {%- set temp= run_query('select to_char(month(getdate()))') -%}
    {% if execute %}
        {% set month=temp.columns[0].values()[0] %}
        {% if month|length ==1 %}
            {%- set month = '0'+month -%}
        {% endif %}
    {% else %}
        {% set month=None %}
    {% endif %}
{% else %}
    {%- set month = var('month') -%}
    {% if month|length ==1 %}
            {%- set month = '0'+month -%}
    {% endif %}
{% endif %})
{% if var('day')=='' %}
    {%- set temp= run_query("select to_char(day(convert_timezone('UTC', current_timestamp())))") -%}
    {% if execute %}
        {% set day=temp.columns[0].values()[0] %}
        {% if day|length ==1 %}
            {%- set day = '0'+day -%}
        {% endif %}
    {% else %}
        {% set day=None %}
    {% endif %}
{% else %}
    {%- set day = var('day') -%}

    {% if day|length ==1 %}
            {%- set day = '0'+day -%}
    {% endif %}

{% endif %}

{{return ([year,month,day])}}

{%- endmacro %}
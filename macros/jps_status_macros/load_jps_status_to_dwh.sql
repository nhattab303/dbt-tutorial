{% macro load_jps_status_to_dwh(src_stg_table) -%}

    select
        job_id,
        created_date,
        close_date,
        status,
        last_checked,
        load_date,
        current_timestamp() as dwh_created_at,
        current_timestamp() as dwh_updated_at

    from
        {{ ref(src_stg_table) }} isu
    {% if is_incremental() %}
        where
            isu.stg_updated_at>(select ifnull(max(dwh_updated_at), date('1970-01-01'))
                        from {{this}})
    {% endif %}

{%- endmacro %}
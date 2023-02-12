-- STG
{% set source_name_var='ods_jps_status' %}
{% set source_table_var= 'ods_indeed_' + var("site")|string|lower + '_status_test6' %}
{% set table_alias_var= 'stg_indeed_' + var("site")|string|lower + '_jps_status' %}

{{ config(materialized='incremental', alias=table_alias_var, unique_key='job_id',
merge_update_columns=['close_date', 'is_closed', 'last_checked', 'load_date', 'stg_updated_at']) }}

with delta_ids_cte as
(
    select
        distinct job_id as id
    from
        {{ source(source_name_var, source_table_var) }}
--        {{ ref('ods_indeed_job_post_status') }}
    where load_date > (
        select
            ifnull(max(stg_created_at), date('1970-01-01'))
        from
            {{this}}
    )
),

ods_cte as
(
    select
        json_data:job_id as job_id,
        try_to_date(json_data:close_date::string) as close_date,
        json_data:creation_date as created_date,
        json_data:status='closed' as is_closed,
        try_to_timestamp(json_data:time_checked::string) as last_checked,
        load_date
    from
        {{source(source_name_var, source_table_var)}}
--        {{ ref('ods_indeed_job_post_status') }}
    where
        json_data:status in ('closed', 'open')
),

aggregated_result_cte as
(
    select
        job_id,
        min(ods.close_date) as close_date,
        min(ods.created_date) as created_date,
        iff(boolor_agg(ods.is_closed), 'closed', 'open') as status,
        max(ods.last_checked) as last_checked,
        max(ods.load_date) as load_date
    from
        ods_cte ods
    inner join delta_ids_cte di
        on di.id = ods.job_id
    group by job_id
)

select
    job_id,
    close_date,
    created_date,
    status,
    last_checked,
    load_date,
    current_timestamp() as stg_created_at,
    current_timestamp() as stg_updated_at
from
    aggregated_result_cte arc
    {% if is_incremental() %}
        where
            arc.load_date>(select ifnull(max(d.stg_updated_at), date('1970-01-01'))
                        from {{this}} d)
    {% endif %}

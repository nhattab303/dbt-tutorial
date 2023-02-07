-- STG
{% set source_name_var='ods_jps_status' %}
{% set source_table_var= 'ods_indeed_' + var("site")|string|lower + '_jps_status`' %}
{% set table_alias_var= 'stg_indeed_' + var("site")|string|lower + '_jps_status' %}

{{ config(materialized='incremental', alias=table_alias_var, unique_key='job_id', schema='stg') }}

with delta_ids_cte as
(
    select
        distinct job_id as id
    from
        {{ source(source_name_var, source_table_var) }}
    where load_date > (
        select
            ifnull(max(stg_created_at), date('1970-01-01'))
        from
            {{this}}
    )
) di

select
    job_id,
    min(try_to_date(json_data:close_date::string)) as close_date,
    min(json_data:creation_date) as created_date,
    boolor_agg(json_data:status='closed') as is_closed,
    max(try_to_timestamp(json_data:time_checked::string)) as last_checked,
    max(load_date) as load_date,
    current_timestamp() as stg_updated_at
from
    {{ref(source_table_var)}} ods
inner join di
    on di.id = ods.job_id
    where json_data:status in ('closed', 'open')
    {% if is_incremental() %}
        and
            ods.load_date>(select ifnull(max(d.stg_updated_at), date('1970-01-01'))
                        from {{this}} d)
    {% endif %}
group by job_id

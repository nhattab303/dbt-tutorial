{% macro load_jps_to_ods(source_gcs_stage, ods_table) -%}

{% set dates_arr=get_date_macro() %}

{% set source_name_var='ods_jps_status' %}


copy into {{source(source_name_var, ods_table|string|lower)}}
from(
    select
        parse_json($1) as json_data,
        json_data:job_id as job_id,
        json_data:source as source,
        metadata$filename as file_name,
        metadata$file_row_number as row_number,
        current_timestamp() as load_date
    from
        {{var('bucket_datasets_stage')}}/job_boards/{{source_gcs_stage|string|lower}}/status/{{dates_arr[0]}}/{{dates_arr[1]}}/{{dates_arr[2]}}/(
           file_format => {{var('json_ff')}} ,pattern=>'.*json'
        )
 )

{%- endmacro %}
{% macro load_jps_to_ods() -%}

{% set dates_arr=get_date_macro() %}

{% set source_name_var='ods_jps_status' %}
{% set source_table_var= 'ods_indeed_' + var("site")|string|lower + '_status_test6' %}


copy into {{source(source_name_var, source_table_var)}}
from(
    select
        parse_json($1) as json_data,
        json_data:job_id as job_id,
        json_data:source as source,
        metadata$filename as file_name,
        metadata$file_row_number as row_number,
        current_timestamp() as load_date
    from
        {{var('bucket_datasets_stage')}}/{{'job_boards/indeed/'}}{{var('site')|string|lower}}{{'/status'}}/{{dates_arr[0]}}/{{dates_arr[1]}}/{{dates_arr[2]}}/(
           file_format => {{var('json_ff')}} ,pattern=>'.*json'
        )
 )


{%- endmacro %}
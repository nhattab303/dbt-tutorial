{{ config(materialized='raw_sql',schema='ods')}}

{% set dates_arr=get_date_macro() %}

copy into {{source('ODS_INDEED_IL_JOB_POST_STATUS','ODS_INDEED_IL_STATUS')}}
from(
    select
        parse_json($1):id as job_id,
        parse_json($1) as json_data,
        metadata$filename as file_name,
        metadata$file_row_number as row_number,
        current_timestamp() as load_date
    from
        {{var('bucket_datasets_stage')}}/{{'indeed/il/status'}}"/{{dates_arr[0]}}/{{dates_arr[1]}}/{{dates_arr[2]}}(
           file_format => {{var('json_ff')}} ,pattern=>'.*json'
        )
 );
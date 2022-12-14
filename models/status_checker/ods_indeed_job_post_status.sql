{{ config(materialized='raw_sql',schema='ods')}}

/*
# ODS Indeed Extractor Status Loader

This model loads status files that were generated by Indeed extractor from GCS to ODS database.
Expects site variable.
site variable can be one of ['IL', 'UAE', 'US']
*/

{% set dates_arr=get_date_macro() %}

{% set source_name='ODS_INDEED_STATUS' %}
{% set source_table= 'ODS_INDEED_' + var("site")|string|upper + '_STATUS_TEST2' %}

copy into {{source(source_name, source_table)}}
from(
    select
        parse_json($1):job_id as job_id,
        parse_json($1):status as status,
        try_to_date(parse_json($1):close_date::string) as close_date,
        try_to_date(parse_json($1):time_checked::string) as time_checked,
        parse_json($1):time_updated as time_updated,
        parse_json($1):creation_date as date_created,
        parse_json($1):source as source,
        parse_json($1) as json_data,
        metadata$filename as file_name,
        metadata$file_row_number as row_number,
        current_timestamp() as load_date
    from
        {{var('bucket_datasets_stage')}}/{{'job_boards/indeed/'}}{{var('site')|string|lower}}{{'/status'}}/2023/01/11/(
           file_format => {{var('json_ff')}} ,pattern=>'.*json'
        )
 )
 FORCE = TRUE
 ;
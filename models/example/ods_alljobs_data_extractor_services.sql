{{ config(materialized='raw_sql',schema='ods')}}

{% set dates_arr=get_date_macro() %}

copy into {{source('ods_job_posts','ods_alljobs_data_extractor_services')}}
from(
    select
        parse_json($1):job_id as job_id,
        parse_json($1):title as title,
        parse_json($1):description as description,
        parse_json($1):category as category,
        parse_json($1):category_id as category_id,
        parse_json($1):subcategory as subcategory,
        parse_json($1):subcategory_id as subcategory_id,
        parse_json($1):status as status,
        parse_json($1):date_posted as date_posted,
        parse_json($1):employment_type as employment_type -- list
        parse_json($1):job_location as job_location -- list
        parse_json($1):industry as industry,
        parse_json($1):hiring_organization as hiring_organization -- {"logo": "", "company_job_post_page": ""}
        parse_json($1):last_update_date as ,
        parse_json($1):requirements as requirements -- list
        parse_json($1):name as name,
        parse_json($1):valid_through as valid_through,
        metadata$filename as file_name,
        metadata$file_row_number as row_number,
        current_timestamp() as load_date
    from
        {{var('bucket_des_jobposts')}}/{{dates_arr[0]}}/{{dates_arr[1]}}/{{dates_arr[2]}}(
           file_format => {{var('json_ff')}} ,pattern=>'.*json'
        )
 );
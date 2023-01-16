-- STG
{% set source_name='ODS_INDEED_STATUS' %}
{% set source_table= 'ODS_INDEED_' + var("site")|string|upper + '_STATUS_TEST3' %}
{% set table_alias= 'ODS_INDEED_' + var("site")|string|upper + '_STATUS_STG' %}

{{ config(materialized='incremental', alias=table_alias) }}

with delta_dups as
(
    SELECT
        DISTINCT JOB_ID as JOB_ID
    FROM
        {{ source(source_name, source_table) }}
    WHERE load_date > (
        SELECT
            IFNULL(max(stg_created_at), date('1970-01-01'))
        FROM
            {{this}}
    )
),
ODS AS (
    SELECT
        JOB_ID AS JOB_ID,
        MIN(try_to_date(parse_json($1):close_date::string)) AS close_date,
        MIN(parse_json($1):creation_date) AS creation_date,
        BOOLOR_AGG(IS_CLOSED) AS IS_CLOSED,
        MAX(try_to_date(parse_json($1):time_checked::string)) AS time_checked,
        MAX(load_date) AS load_date,
        current_timestamp() AS stg_created_at
    FROM
        {{source(source_name, source_table)}}
)
SELECT * FROM ODS
INNER JOIN delta_dups
    ON DELTA_DUPS.JOB_ID=ODS.JOB_ID
{% if is_incremental() %}
    where
        ODS.stg_created_at>(select max(d.stg_created_at)
                    from {{this}} d )
{% endif %}
GROUP BY JOB_ID

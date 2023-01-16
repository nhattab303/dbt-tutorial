-- stg
{% set source_name='ods_indeed_status' %}
{% set source_table= 'ods_indeed_' + var("site")|string|lower + '_status_test3' %}
{% set table_alias= 'ods_indeed_' + var("site")|string|lower + '_status_stg' %}

{{ config(materialized='incremental', alias=table_alias, unique_key='JOB_ID') }}

with delta_dups as
(
    SELECT
        DISTINCT JOB_ID as ID
    FROM
        {{ source(source_name, source_table) }}
    WHERE load_date > (
        SELECT
            IFNULL(max(stg_created_at), date('1970-01-01'))
        FROM
            {{this}}
    )
),
ODS AS (SELECT * FROM {{source(source_name, source_table)}})

SELECT
    JOB_ID AS JOB_ID,
    MIN(TRY_TO_TIMESTAMP(parse_json(json_data):close_date::string)) AS CLOSE_DATE,
    MIN(parse_json(json_data):creation_date) AS CREATION_DATE,
    BOOLOR_AGG(IS_CLOSED) AS IS_CLOSED,
    MAX(TRY_TO_TIMESTAMP(parse_json(json_data):time_checked::string)) AS LAST_CHECKED,
    MAX(load_date) AS LOAD_DATE,
    current_timestamp() AS STG_CREATED_AT
FROM
    ODS
INNER JOIN DELTA_DUPS
    ON DELTA_DUPS.ID=ODS.JOB_ID
{% if is_incremental() %}
    WHERE
        ODS.LOAD_DATE>(SELECT IFNULL(MAX(d.STG_CREATED_AT), date('1970-01-01'))
                    FROM {{this}} d )
{% endif %}
GROUP BY JOB_ID
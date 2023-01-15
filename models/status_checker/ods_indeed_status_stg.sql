-- STG
{{ config(materialized='incremental', schema='ODS') }}

{% set source_name='ODS_INDEED_STATUS' %}
{% set source_table= 'ODS_INDEED_' + var("site")|string|upper + '_STATUS_TEST3' %}

with delta_dups as
(
    SELECT
        DISTINCT job_id
    FROM
        {{ source(source_name, source_table) }}
    WHERE load_date > (
        SELECT
            IFNULL(max(stg_updated_at), date('1970-01-01'))
        FROM
            {{this}}
    )
),
ODS AS (
    SELECT
        job_id AS job_id,
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
    ON delta_dups.job_id=ods.job_id
GROUP BY job_id
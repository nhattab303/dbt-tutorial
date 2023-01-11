
{% snapshot ods_indeed_job_post_status_snapshot %}

{% set source_name='ODS_INDEED_STATUS' %}
{% set source_table= 'ODS_INDEED_' + var("site")|string|upper + '_STATUS_TEST2' %}


{{
    config(
      target_database='DEV_DB',
      target_schema='ODS',
      unique_key='job_id',
      strategy='timestamp',
      updated_at='time_updated',
    )
}}

select * from {{ source(source_name, source_table) }}

{% endsnapshot %}
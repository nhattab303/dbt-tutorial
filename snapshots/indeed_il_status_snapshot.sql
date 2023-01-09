{% snapshot ods_indeed_job_post_status %}

{{
    config(
      target_database='DEV_DB',
      target_schema='snapshots',
      unique_key='job_id',
      strategy='timestamp',
      updated_at='time_updated',
    )
}}

select * from {{ ref('ODS_INDEED_IL_STATUS_TEST1') }}

{% endsnapshot %}
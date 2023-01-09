{% snapshot indeed_job_post_status %}

{{
    config(
      target_database='DEV_DB',
      target_schema='snapshots',
      unique_key='job_id',
      strategy='timestamp',
      updated_at='time_updated',
    )
}}

select * from {{ source('LOADED_ODS_INDEED_IL_JOB_POST_STATUS', 'ODS_INDEED_IL_JOB_POST_STATUS') }}

{% endsnapshot %}
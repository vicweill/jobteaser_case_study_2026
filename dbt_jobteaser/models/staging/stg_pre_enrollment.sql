{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'pre_enrollment') }}
),

cleaned as (
    select
        id as pre_enrollment_id,
        course_id,
        user_id,
        created_at,
        auto_enroll
    from source
)

select * from cleaned

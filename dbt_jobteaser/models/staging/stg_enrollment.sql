{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'enrollment') }}
),

cleaned as (
    select
        id as enrollment_id,
        course_id,
        user_id,
        enrolled_at,
        is_active,
        enrollment_mode
    from source
)

select * from cleaned

{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'courses') }}
),

cleaned as (
    select
        course_key,
        lower(course_name) as course_name,
        starts_at,
        lower(course_language) as course_language,
        course_effort
    from source
)

select * from cleaned

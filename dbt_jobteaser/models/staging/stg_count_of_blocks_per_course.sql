{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'count_of_blocks_per_course') }}
),

cleaned as (
    select
        course_key,
        block_count
    from source
)

select * from cleaned

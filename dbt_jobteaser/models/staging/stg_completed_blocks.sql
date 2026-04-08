{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'completed_blocks') }}
),

cleaned as (
    select
        id as completed_block_id,
        course_key,
        block_key,
        block_type,
        user_id,
        completed_at
    from source
)

select * from cleaned

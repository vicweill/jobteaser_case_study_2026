{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'users_in_jobteaser') }}
),

cleaned as (
    select
        id as jobteaser_user_id,
        lower(email) as email,
        user_created_at,
        user_deleted_at,
        last_sign_in
    from source
)

select * from cleaned

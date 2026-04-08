{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'users_in_career_explorer') }}
),

cleaned as (
    select
        id as career_explorer_user_id,
        last_login_at,
        is_superuser,
        is_staff,
        is_active,
        created_at,
        lower(email) as email
    from source
)

select * from cleaned

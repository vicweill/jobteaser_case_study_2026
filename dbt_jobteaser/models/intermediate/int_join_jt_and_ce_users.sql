{{ config(materialized='view') }}

with join_users as (
    select
        jt.jobteaser_user_id,
        ce.career_explorer_user_id,
        jt.email
        -- CHECK IF WE ONLY HAVE 1 ROW PER JT USER
    from {{ ref('stg_users_in_jobteaser') }} jt
    inner join {{ ref('stg_users_in_career_explorer') }} ce
        on jt.email = ce.email
)

select * from join_users

{{ config(materialized='table') }}

-- We could create a test that checks if the rate is always between 0 and 100

with new_ce_users as (
    select
        date_trunc('month', created_at) as registration_month,
        coalesce(count(career_explorer_user_id), 0) as new_users
    from {{ ref('stg_users_in_career_explorer') }}
    where is_superuser = false
      and is_staff = false
    group by registration_month
),

cumulative_ce_users as (
    select
        registration_month,
        sum(new_users) over (order by registration_month asc) as cumsum_users
    from new_ce_users
    order by registration_month asc
),

nb_applications as (
    select
        date_trunc('month', a.application_date_time) as month,
        count(distinct a.applicant_id) as nb_applicants
    from {{ ref('stg_applications_in_jobteaser') }} a
    inner join {{ ref('int_join_jt_and_ce_users') }} j on a.applicant_id = j.jobteaser_user_id
    group by month
),

compute_query as (
    select
        a.month,
        a.nb_applicants,
        last_value(c.cumsum_users ignore nulls) over (order by a.month asc) as cumsum_users
    from nb_applications a
    left join cumulative_ce_users c on a.month = c.registration_month
)

select
    month,
    nb_applicants,
    cumsum_users,
    case
        when cumsum_users > 0 then nb_applicants * 1.0 / cumsum_users
        else 0
    end as application_rate
from compute_query
where month > '2025-01-01'
order by month desc

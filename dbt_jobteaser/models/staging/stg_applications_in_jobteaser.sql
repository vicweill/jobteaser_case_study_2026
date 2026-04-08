{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'applications_in_jobteaser') }}
),

cleaned as (
    select
        application_id,
        applicant_id,
        application_date_time,
        ip_country_code,
        device,
        job_ad_id,
        job_ad_language,
        job_ad_contract_type,
        company_industry,
        position_category_name,
        parent_position_category_name
    from source
)

select * from cleaned

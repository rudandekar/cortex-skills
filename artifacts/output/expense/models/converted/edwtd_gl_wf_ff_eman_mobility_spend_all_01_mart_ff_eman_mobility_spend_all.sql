{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_eman_mobility_spend_all', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_EMAN_Mobility_Spend_All',
        'target_table': 'FF_EMAN_MOBILITY_SPEND_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.146139+00:00'
    }
) }}

WITH 

source_ff_eman_mobility_spend_all_ftp AS (
    SELECT
        emplid,
        cec_userid,
        dept_name,
        deptid,
        vendor,
        service_type,
        telnum,
        bill_month,
        amount_usd,
        lastrefreshdate
    FROM {{ source('raw', 'ff_eman_mobility_spend_all_ftp') }}
),

transformed_exp_eman_mobility AS (
    SELECT
    emplid,
    cec_userid,
    dept_name,
    deptid,
    vendor,
    service_type,
    telnum,
    bill_month,
    amount_usd,
    lastrefreshdate,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_ff_eman_mobility_spend_all_ftp
),

final AS (
    SELECT
        emplid,
        cec_userid,
        dept_name,
        deptid,
        vendor,
        service_type,
        telnum,
        bill_month,
        amount_usd,
        lastrefreshdate,
        create_datetime,
        action_code
    FROM transformed_exp_eman_mobility
)

SELECT * FROM final
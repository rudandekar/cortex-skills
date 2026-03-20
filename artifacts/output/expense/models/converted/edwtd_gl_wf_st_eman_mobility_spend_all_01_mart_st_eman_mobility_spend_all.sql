{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_eman_mobility_spend_all', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_EMAN_Mobility_Spend_All',
        'target_table': 'st_eman_mobility_spend_all',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.951312+00:00'
    }
) }}

WITH 

source_ff_eman_mobility_spend_all AS (
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
    FROM {{ source('raw', 'ff_eman_mobility_spend_all') }}
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
    FROM source_ff_eman_mobility_spend_all
)

SELECT * FROM final
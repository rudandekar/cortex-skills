{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_rae_net_revenue_rao', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_SM_RAE_NET_REVENUE_RAO',
        'target_table': 'SM_RAE_NET_REVENUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.213827+00:00'
    }
) }}

WITH 

source_sm_rae_net_revenue AS (
    SELECT
        net_revenue_key,
        sk_net_revenue_id,
        ss_cd,
        schedule_line_id,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_rae_net_revenue') }}
),

final AS (
    SELECT
        net_revenue_key,
        sk_net_revenue_id,
        ss_cd,
        schedule_line_id,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_rae_net_revenue
)

SELECT * FROM final
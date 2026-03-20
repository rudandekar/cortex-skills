{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcfi_rae_net_revenue', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_XXCFI_RAE_NET_REVENUE',
        'target_table': 'EL_XXCFI_RAE_NET_REVENUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.486645+00:00'
    }
) }}

WITH 

source_el_xxcfi_rae_net_revenue AS (
    SELECT
        net_revenue_id,
        code_combination_id,
        gl_date,
        gl_posted_date,
        amount,
        acctd_amount,
        currency_code,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_code
    FROM {{ source('raw', 'el_xxcfi_rae_net_revenue') }}
),

final AS (
    SELECT
        net_revenue_id,
        code_combination_id,
        gl_date,
        gl_posted_date,
        amount,
        acctd_amount,
        currency_code,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_code
    FROM source_el_xxcfi_rae_net_revenue
)

SELECT * FROM final
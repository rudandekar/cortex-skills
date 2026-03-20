{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_decision_stg23nf', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_DECISION_STG23NF',
        'target_table': 'N_DEAL_DECISION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.936935+00:00'
    }
) }}

WITH 

source_n_deal_decision AS (
    SELECT
        bk_deal_decision_name,
        active_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_deal_decision') }}
),

final AS (
    SELECT
        bk_deal_decision_name,
        active_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_deal_decision
)

SELECT * FROM final
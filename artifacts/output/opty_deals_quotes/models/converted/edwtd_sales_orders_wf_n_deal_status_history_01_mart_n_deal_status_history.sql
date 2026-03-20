{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_status_history', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_STATUS_HISTORY',
        'target_table': 'N_DEAL_STATUS_HISTORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.959893+00:00'
    }
) }}

WITH 

source_n_deal_status_history AS (
    SELECT
        bk_deal_status_cd,
        bk_deal_id,
        bk_status_trx_dtm,
        bk_deal_status_ss_cd,
        dv_status_trx_dt,
        cisco_worker_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_deal_status_history') }}
),

final AS (
    SELECT
        bk_deal_status_cd,
        bk_deal_id,
        bk_status_trx_dtm,
        bk_deal_status_ss_cd,
        dv_status_trx_dt,
        cisco_worker_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_deal_status_history
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rebok_ms_es_itm_sls_trx', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_N_REBOK_MS_ES_ITM_SLS_TRX',
        'target_table': 'N_REBOK_MS_ES_ITM_SLS_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.258912+00:00'
    }
) }}

WITH 

source_w_rebok_ms_es_itm_sls_trx AS (
    SELECT
        so_sbscrptn_itm_sls_trx_key,
        ms_partner_pty_key,
        modified_by_user_id,
        ms_identification_method_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rebok_ms_es_itm_sls_trx') }}
),

final AS (
    SELECT
        so_sbscrptn_itm_sls_trx_key,
        ms_partner_pty_key,
        modified_by_user_id,
        ms_identification_method_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_rebok_ms_es_itm_sls_trx
)

SELECT * FROM final
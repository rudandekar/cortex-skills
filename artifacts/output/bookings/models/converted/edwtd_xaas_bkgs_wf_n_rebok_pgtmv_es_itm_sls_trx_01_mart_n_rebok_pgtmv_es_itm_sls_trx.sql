{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rebok_pgtmv_es_itm_sls_trx', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_N_REBOK_PGTMV_ES_ITM_SLS_TRX',
        'target_table': 'N_REBOK_PGTMV_ES_ITM_SLS_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.885862+00:00'
    }
) }}

WITH 

source_w_rebok_pgtmv_es_itm_sls_trx AS (
    SELECT
        so_sbscrptn_itm_sls_trx_key,
        partner_pty_key,
        partner_country_party_key,
        src_rptdrbk_pgtmv_be_geoid_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        partner_certification_cd,
        action_code,
        dml_type,
        partner_assignment_code_ss_code,
        partner_assignment_code,
        partner_override_reason_code
    FROM {{ source('raw', 'w_rebok_pgtmv_es_itm_sls_trx') }}
),

final AS (
    SELECT
        so_sbscrptn_itm_sls_trx_key,
        partner_pty_key,
        partner_country_party_key,
        src_rptdrbk_pgtmv_be_geoid_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        partner_certification_cd,
        partner_assignment_code_ss_code,
        partner_assignment_code,
        partner_override_reason_code
    FROM source_w_rebok_pgtmv_es_itm_sls_trx
)

SELECT * FROM final
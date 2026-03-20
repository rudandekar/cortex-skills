{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rebok_ms_enrichment_mnl_trx', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_REBOK_MS_ENRICHMENT_MNL_TRX',
        'target_table': 'W_REBOK_MS_ENRICHMENT_MNL_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.361990+00:00'
    }
) }}

WITH 

source_ex_otm_phx_trx_mnl AS (
    SELECT
        trx_id,
        order_line_id,
        trx_orig_code,
        party_ext_id,
        enrichment_code,
        enrichment_party_type,
        pgtmv_party_be_id,
        class_code,
        user_name,
        creation_date,
        last_update_date,
        batch_id,
        exception_type
    FROM {{ source('raw', 'ex_otm_phx_trx_mnl') }}
),

source_st_otm_phx_trx_mnl AS (
    SELECT
        trx_id,
        order_line_id,
        trx_orig_code,
        party_ext_id,
        enrichment_code,
        enrichment_party_type,
        pgtmv_party_be_id,
        class_code,
        user_name,
        creation_date,
        last_update_date,
        batch_id
    FROM {{ source('raw', 'st_otm_phx_trx_mnl') }}
),

final AS (
    SELECT
        manual_trx_key,
        ms_partner_party_key,
        ms_identification_method_int,
        modified_by_user_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_otm_phx_trx_mnl
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_phx_trx_mnl_xaas', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_PHX_TRX_MNL_XAAS',
        'target_table': 'ST_OTM_PHX_TRX_MNL_XAAS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.308666+00:00'
    }
) }}

WITH 

source_ff_otm_phx_trx_mnl_xaas AS (
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
    FROM {{ source('raw', 'ff_otm_phx_trx_mnl_xaas') }}
),

final AS (
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
    FROM source_ff_otm_phx_trx_mnl_xaas
)

SELECT * FROM final
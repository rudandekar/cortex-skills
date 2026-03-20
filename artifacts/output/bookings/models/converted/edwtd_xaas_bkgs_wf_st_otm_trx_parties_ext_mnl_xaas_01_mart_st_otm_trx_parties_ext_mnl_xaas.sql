{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_trx_parties_ext_mnl_xaas', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_TRX_PARTIES_EXT_MNL_XAAS',
        'target_table': 'ST_OTM_TRX_PARTIES_EXT_MNL_XAAS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.396235+00:00'
    }
) }}

WITH 

source_ff_otm_trx_parties_ext_mnl_xaas AS (
    SELECT
        trx_party_ext_id,
        order_line_id,
        trx_orig_code,
        enrichment_code,
        enrichment_party_type,
        hz_cr_party_id,
        sav_id,
        split_percent,
        pgtmv_party_be_id,
        pgtmv_party_geo_id,
        pgtmv_party_country_id,
        start_date,
        status_code,
        creation_date,
        user_name,
        validation_date,
        last_update_date,
        batch_id,
        pgtmv_partner_cert_level,
        partner_assignment_code_ss_code,
        partner_assignment_code,
        partner_override_reason_code
    FROM {{ source('raw', 'ff_otm_trx_parties_ext_mnl_xaas') }}
),

final AS (
    SELECT
        trx_party_ext_id,
        order_line_id,
        trx_orig_code,
        enrichment_code,
        enrichment_party_type,
        hz_cr_party_id,
        sav_id,
        split_percent,
        pgtmv_party_be_id,
        pgtmv_party_geo_id,
        pgtmv_party_country_id,
        start_date,
        status_code,
        creation_date,
        user_name,
        validation_date,
        last_update_date,
        batch_id,
        pgtmv_partner_cert_level,
        partner_assignment_code_ss_code,
        partner_assignment_code,
        partner_override_reason_code
    FROM source_ff_otm_trx_parties_ext_mnl_xaas
)

SELECT * FROM final
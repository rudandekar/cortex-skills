{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_geo_acct_es_itm_sls_trx', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_W_GEO_ACCT_ES_ITM_SLS_TRX',
        'target_table': 'ST_OTM_TRX_PARTIES_EXT_MNL_XAAS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.816705+00:00'
    }
) }}

WITH 

source_ex_otm_trx_partie_ext_mnl_xaas AS (
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
        exception_type
    FROM {{ source('raw', 'ex_otm_trx_partie_ext_mnl_xaas') }}
),

source_st_otm_trx_parties_ext_mnl_xaas AS (
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
        partner_assignment_code_ss_code,
        partner_assignment_code,
        partner_override_reason_code
    FROM {{ source('raw', 'st_otm_trx_parties_ext_mnl_xaas') }}
),

source_w_geo_acct_es_itm_sls_trx AS (
    SELECT
        so_sbscrptn_itm_sls_trx_key,
        sales_acct_group_pty_key,
        field_validated_cust_pty_key,
        end_cust_ownership_split_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_geo_acct_es_itm_sls_trx') }}
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
    FROM source_w_geo_acct_es_itm_sls_trx
)

SELECT * FROM final
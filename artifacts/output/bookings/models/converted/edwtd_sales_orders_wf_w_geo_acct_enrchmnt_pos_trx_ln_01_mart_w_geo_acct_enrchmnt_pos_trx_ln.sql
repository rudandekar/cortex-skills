{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_geo_acct_enrchmnt_pos_trx_ln', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_GEO_ACCT_ENRCHMNT_POS_TRX_LN',
        'target_table': 'W_GEO_ACCT_ENRCHMNT_POS_TRX_LN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.067308+00:00'
    }
) }}

WITH 

source_st_otm_trx_parties_ext_pos AS (
    SELECT
        trx_party_ext_id,
        pos_trans_id,
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
    FROM {{ source('raw', 'st_otm_trx_parties_ext_pos') }}
),

source_ex_otm_trx_parties_ext_pos AS (
    SELECT
        trx_party_ext_id,
        pos_trans_id,
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
        exception_type,
        pgtmv_partner_cert_level,
        partner_assignment_code_ss_code,
        partner_assignment_code,
        partner_override_reason_code
    FROM {{ source('raw', 'ex_otm_trx_parties_ext_pos') }}
),

final AS (
    SELECT
        bk_pos_transaction_id_int,
        field_validated_cust_party_key,
        sales_account_group_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_ex_otm_trx_parties_ext_pos
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rebok_pgtmv_enrchmnt_so_line', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_REBOK_PGTMV_ENRCHMNT_SO_LINE',
        'target_table': 'W_REBOK_PGTMV_ENRCHMNT_SO_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.466415+00:00'
    }
) }}

WITH 

source_st_otm_trx_parties_ext_sol AS (
    SELECT
        trx_party_ext_id,
        order_line_id,
        global_name,
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
    FROM {{ source('raw', 'st_otm_trx_parties_ext_sol') }}
),

source_ex_otm_trx_parties_ext_sol AS (
    SELECT
        trx_party_ext_id,
        order_line_id,
        global_name,
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
    FROM {{ source('raw', 'ex_otm_trx_parties_ext_sol') }}
),

final AS (
    SELECT
        sales_order_line_key,
        rebok_rprt_pgtmv_be_geo_id_int,
        rebok_pgtmv_partner_party_key,
        rebok_pgtmv_ptnr_cntry_pty_key,
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
    FROM source_ex_otm_trx_parties_ext_sol
)

SELECT * FROM final
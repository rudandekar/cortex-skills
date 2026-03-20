{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_quote_partner_contact_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_QUOTE_PARTNER_CONTACT_RNWL',
        'target_table': 'N_DEAL_QUOTE_PARTNER_CONTACT_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.948102+00:00'
    }
) }}

WITH 

source_w_deal_quote_partner_contact_rnwl AS (
    SELECT
        bk_quote_num,
        deal_quote_partner_contact_key,
        quote_partner_contact_cco_id,
        quote_prtnr_contact_first_name,
        partner_site_party_key,
        quote_prtnr_contact_last_name,
        bk_quote_partner_email_id,
        qt_ptnr_cntct_job_title_name,
        quote_ptnr_cntct_type_cd,
        primary_flg,
        sk_object_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        quote_ptnr_cntct_phone_num,
        src_created_dtm,
        dv_src_created_dt,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        quote_ptnr_website_url_addr,
        ss_code,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_quote_partner_contact_rnwl') }}
),

final AS (
    SELECT
        bk_quote_num,
        deal_quote_partner_contact_key,
        quote_partner_contact_cco_id,
        quote_prtnr_contact_first_name,
        partner_site_party_key,
        quote_prtnr_contact_last_name,
        bk_quote_partner_email_id,
        qt_ptnr_cntct_job_title_name,
        quote_ptnr_cntct_type_cd,
        primary_flg,
        sk_object_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        quote_ptnr_cntct_phone_num,
        src_created_dtm,
        dv_src_created_dt,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        quote_ptnr_website_url_addr,
        ss_code
    FROM source_w_deal_quote_partner_contact_rnwl
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_quote_partner', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_QUOTE_PARTNER',
        'target_table': 'N_DEAL_QUOTE_PARTNER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.936682+00:00'
    }
) }}

WITH 

source_w_deal_quote_partner AS (
    SELECT
        bk_quote_num,
        partner_site_party_key,
        ssot_update_dtm,
        source_reported_created_by_id,
        source_reported_updated_by_id,
        source_reported_created_on_dtm,
        source_reported_updated_on_dtm,
        source_reported_partner_name,
        src_reported_begeo_party_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        crp_ptnr_site_pty_key,
        crp_ptnr_deal_submitted_dtm,
        dv_crp_ptnr_deal_submitted_dt,
        crp_ptnr_cnvrt_to_primary_flg,
        ss_code,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_quote_partner') }}
),

final AS (
    SELECT
        bk_quote_num,
        partner_site_party_key,
        ssot_update_dtm,
        source_reported_created_by_id,
        source_reported_updated_by_id,
        source_reported_created_on_dtm,
        source_reported_updated_on_dtm,
        source_reported_partner_name,
        src_reported_begeo_party_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        crp_ptnr_site_pty_key,
        crp_ptnr_deal_submitted_dtm,
        dv_crp_ptnr_deal_submitted_dt,
        crp_ptnr_cnvrt_to_primary_flg,
        ss_code
    FROM source_w_deal_quote_partner
)

SELECT * FROM final
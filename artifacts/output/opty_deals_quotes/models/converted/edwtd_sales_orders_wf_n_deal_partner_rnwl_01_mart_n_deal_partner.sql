{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_partner_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_PARTNER_RNWL',
        'target_table': 'N_DEAL_PARTNER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.870273+00:00'
    }
) }}

WITH 

source_w_deal_partner_rnwl1 AS (
    SELECT
        deal_partner_key,
        bk_deal_id,
        src_rprtd_ptnr_site_pty_name,
        primary_flg,
        src_rprtd_deal_partner_cco_id,
        sk_deal_partner_object_id_int,
        partner_country_party_key,
        partner_site_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        partner_type_cd,
        ss_code,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_partner_rnwl1') }}
),

final AS (
    SELECT
        deal_partner_key,
        bk_deal_id,
        src_rprtd_ptnr_site_pty_name,
        primary_flg,
        src_rprtd_deal_partner_cco_id,
        sk_deal_partner_object_id_int,
        partner_country_party_key,
        partner_site_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        partner_type_cd,
        ss_code
    FROM source_w_deal_partner_rnwl1
)

SELECT * FROM final
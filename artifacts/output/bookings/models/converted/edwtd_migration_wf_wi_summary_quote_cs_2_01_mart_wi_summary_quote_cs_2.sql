{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_summary_quote_cs_2', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_WI_SUMMARY_QUOTE_CS_2',
        'target_table': 'WI_SUMMARY_QUOTE_CS_2',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.694375+00:00'
    }
) }}

WITH 

source_wi_summary_quote_cs_2 AS (
    SELECT
        ib_svc_lvl,
        svc_cntrct_ln_tech_svcs_key,
        ip_key,
        bk_service_contract_num,
        service_status_cd,
        maintenance_order_num,
        maintenance_list_local_amt,
        maintenance_net_local_amt,
        sk_id_lint,
        service_product_key,
        source_create_dtm,
        source_application_name,
        case_detail_txt,
        summary_quote_case_num,
        sales_motion_cd,
        bk_svc_cntrct_line_start_dtm,
        svc_cntrct_line_end_dtm,
        terminated_dtm,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        dv_line_style_id_int,
        architecture_id,
        technology_id,
        offer_type_id,
        party_ssot_party_id_int,
        hq_party_ssot_party_id_int,
        as_ts_flag,
        item_categorization,
        covered_inv_item_id,
        covered_product_id
    FROM {{ source('raw', 'wi_summary_quote_cs_2') }}
),

final AS (
    SELECT
        ib_svc_lvl,
        svc_cntrct_ln_tech_svcs_key,
        ip_key,
        bk_service_contract_num,
        service_status_cd,
        maintenance_order_num,
        maintenance_list_local_amt,
        maintenance_net_local_amt,
        sk_id_lint,
        service_product_key,
        source_create_dtm,
        source_application_name,
        case_detail_txt,
        summary_quote_case_num,
        sales_motion_cd,
        bk_svc_cntrct_line_start_dtm,
        svc_cntrct_line_end_dtm,
        terminated_dtm,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        dv_line_style_id_int,
        architecture_id,
        technology_id,
        offer_type_id,
        party_ssot_party_id_int,
        hq_party_ssot_party_id_int,
        as_ts_flag,
        item_categorization,
        covered_inv_item_id,
        covered_product_id
    FROM source_wi_summary_quote_cs_2
)

SELECT * FROM final
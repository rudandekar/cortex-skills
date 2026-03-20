{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_pf_band_threshold', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_PF_BAND_THRESHOLD',
        'target_table': 'N_DEAL_PF_BAND_THRESHOLD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.984296+00:00'
    }
) }}

WITH 

source_w_deal_pf_band_threshold AS (
    SELECT
        bk_deal_id,
        bk_src_rptd_prdt_family_name,
        bk_discount_band_cd,
        bk_src_rptd_pf_dscntruleid_int,
        bk_dv_prdt_family_id,
        pf_approval_band_cd,
        final_approval_band_cd,
        src_rptd_pf_apprvl_rule_id_int,
        created_on_dtm,
        dv_created_on_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_pf_band_threshold') }}
),

final AS (
    SELECT
        bk_deal_id,
        bk_src_rptd_prdt_family_name,
        bk_discount_band_cd,
        bk_src_rptd_pf_dscntruleid_int,
        bk_dv_prdt_family_id,
        pf_approval_band_cd,
        final_approval_band_cd,
        src_rptd_pf_apprvl_rule_id_int,
        created_on_dtm,
        dv_created_on_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_deal_pf_band_threshold
)

SELECT * FROM final
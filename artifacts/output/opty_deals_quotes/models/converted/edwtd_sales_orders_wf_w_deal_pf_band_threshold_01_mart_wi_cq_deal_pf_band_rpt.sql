{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_pf_band_threshold', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_PF_BAND_THRESHOLD',
        'target_table': 'wi_cq_deal_pf_band_rpt',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.914090+00:00'
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

source_st_cq_deal_pf_band_rpt AS (
    SELECT
        batch_id,
        opty_number,
        deal_object_id,
        product_family,
        pf_discount_rule_id,
        pf_discount_band,
        created_on,
        last_run_date,
        deal_component,
        pr_ser_category,
        final_component_band,
        stringent_pf,
        deal_approval_band,
        ext_list_price_gpl,
        extended_list_price,
        extended_net_price,
        pf_approval_rule_id,
        pf_approval_band,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cq_deal_pf_band_rpt') }}
),

source_ex_cq_deal_pf_band_rpt AS (
    SELECT
        batch_id,
        opty_number,
        deal_object_id,
        product_family,
        pf_discount_rule_id,
        pf_discount_band,
        created_on,
        last_run_date,
        deal_component,
        pr_ser_category,
        final_component_band,
        stringent_pf,
        deal_approval_band,
        ext_list_price_gpl,
        extended_list_price,
        extended_net_price,
        pf_approval_rule_id,
        pf_approval_band,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_cq_deal_pf_band_rpt') }}
),

final AS (
    SELECT
        batch_id,
        opty_number,
        deal_object_id,
        product_family,
        pf_discount_rule_id,
        pf_discount_band,
        created_on,
        last_run_date,
        deal_component,
        pr_ser_category,
        final_component_band,
        stringent_pf,
        deal_approval_band,
        ext_list_price_gpl,
        extended_list_price,
        extended_net_price,
        pf_approval_rule_id,
        pf_approval_band,
        create_datetime,
        action_code
    FROM source_ex_cq_deal_pf_band_rpt
)

SELECT * FROM final
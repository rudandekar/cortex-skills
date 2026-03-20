{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_sr_techni_assist_center_cost', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_SM_SR_TECHNI_ASSIST_CENTER_COST',
        'target_table': 'WI_EDW_TS_TAC_COGS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.462386+00:00'
    }
) }}

WITH 

source_wi_edw_ts_tac_cogs AS (
    SELECT
        quarter,
        fiscal_month_id,
        fiscal_month,
        sr_number,
        sa_number,
        work_theater,
        c3_cust_theater,
        srvc_line,
        srvc_offering,
        srvc_category,
        c3_market_segment,
        country_code,
        hw_family,
        ent_product_family,
        hybrid_product_family,
        tac_bb_cost,
        tac_ot_cost,
        tbo_cost,
        oh_cost,
        tac_non_ot_cost,
        sr_type,
        customer,
        problem_code,
        entry_channel,
        delivery_channel,
        acod,
        init_severity,
        tsrt_id,
        case_weight,
        covered_product_family,
        tbc,
        rma_flag,
        warranty_pct,
        tac_allocated_wrty_pct,
        subscription_reference_id,
        virtual_account_id,
        smart_account_id
    FROM {{ source('raw', 'wi_edw_ts_tac_cogs') }}
),

source_sm_sr_techni_assist_center_cost AS (
    SELECT
        service_request_tac_cost_key,
        src_rptd_service_request_num_int,
        bk_sr_tac_cost_fscl_yr_num_int,
        bk_sr_tac_cost_fscl_mth_num_int,
        bk_sr_tac_work_theater_cd,
        delivery_channel_name,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_sr_techni_assist_center_cost') }}
),

final AS (
    SELECT
        quarter,
        fiscal_month_id,
        fiscal_month,
        sr_number,
        sa_number,
        work_theater,
        c3_cust_theater,
        srvc_line,
        srvc_offering,
        srvc_category,
        c3_market_segment,
        country_code,
        hw_family,
        ent_product_family,
        hybrid_product_family,
        tac_bb_cost,
        tac_ot_cost,
        tbo_cost,
        oh_cost,
        tac_non_ot_cost,
        sr_type,
        customer,
        problem_code,
        entry_channel,
        delivery_channel,
        acod,
        init_severity,
        tsrt_id,
        case_weight,
        covered_product_family,
        tbc,
        rma_flag,
        warranty_pct,
        tac_allocated_wrty_pct,
        subscription_reference_id,
        virtual_account_id,
        smart_account_id
    FROM source_sm_sr_techni_assist_center_cost
)

SELECT * FROM final
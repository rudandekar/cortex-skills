{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_allocated_tac_warranty', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_ALLOCATED_TAC_WARRANTY',
        'target_table': 'MT_ALLOCATED_TAC_WARRANTY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.737333+00:00'
    }
) }}

WITH 

source_mt_allocated_tac_warranty AS (
    SELECT
        fiscal_year_month_int,
        warr_fiscal_year_month_int,
        service_contract_line_name,
        pf_valid_flg,
        business_entity_descr,
        sub_business_entity_descr,
        overall_allocated_tac_cost_usd_amt,
        allocated_tac_warranty_usd_amt,
        allocated_tac_non_warranty_usd_amt,
        alloctac_ib_ratio_wty_number,
        dv_fiscal_quarter_id,
        sr_number_int,
        work_theater_cd,
        work_theater_valid_flg,
        entry_channel_name,
        c3_cust_theater_name,
        srvc_line_id,
        srvc_offering_name,
        srvc_category_name,
        customer_name,
        tac_cost,
        tac_warranty_cost,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_allocated_tac_warranty') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        warr_fiscal_year_month_int,
        service_contract_line_name,
        pf_valid_flg,
        business_entity_descr,
        sub_business_entity_descr,
        overall_allocated_tac_cost_usd_amt,
        allocated_tac_warranty_usd_amt,
        allocated_tac_non_warranty_usd_amt,
        alloctac_ib_ratio_wty_number,
        dv_fiscal_quarter_id,
        sr_number_int,
        work_theater_cd,
        work_theater_valid_flg,
        entry_channel_name,
        c3_cust_theater_name,
        srvc_line_id,
        srvc_offering_name,
        srvc_category_name,
        customer_name,
        tac_cost,
        tac_warranty_cost,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_allocated_tac_warranty
)

SELECT * FROM final
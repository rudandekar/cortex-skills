{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_edw_tac_cogs', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_EDW_TAC_COGS',
        'target_table': 'WI_EDW_TAC_COGS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.770859+00:00'
    }
) }}

WITH 

source_wi_edw_tac_cogs AS (
    SELECT
        quarter,
        fiscal_year_month_int,
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
        tsb_cost,
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
    FROM {{ source('raw', 'wi_edw_tac_cogs') }}
),

final AS (
    SELECT
        quarter,
        fiscal_year_month_int,
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
        tsb_cost,
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
    FROM source_wi_edw_tac_cogs
)

SELECT * FROM final
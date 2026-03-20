{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cxpnl_cogs_erly_wrng_audit', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_CXPNL_COGS_ERLY_WRNG_AUDIT',
        'target_table': 'EL_CENTRAL_PL_MAPPING_AUDIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.937827+00:00'
    }
) }}

WITH 

source_el_c3bl_attrib_ctrb_trnd_adt AS (
    SELECT
        validation_type,
        validation_sub_type,
        name,
        criteria,
        phase,
        validated_date,
        fiscal_year_month_int,
        attribute_type,
        tolerance_diff_percentage,
        curr_quantity,
        avg_quantity,
        quantity_diff,
        quantity_diff_pct,
        quantity_result,
        curr_unit_price,
        avg_unit_price,
        unit_price_diff,
        unit_price_diff_pct,
        unit_price_result,
        curr_cost,
        avg_cost,
        cost_diff,
        cost_diff_pct,
        cost_result
    FROM {{ source('raw', 'el_c3bl_attrib_ctrb_trnd_adt') }}
),

source_el_central_pl_mapping_audit AS (
    SELECT
        validation_type,
        validation_sub_type,
        name,
        criteria,
        phase,
        validated_date,
        fiscal_year_month_int,
        direct_flag,
        as_tss_flag,
        cost_type,
        amount
    FROM {{ source('raw', 'el_central_pl_mapping_audit') }}
),

source_el_rma_tac_gl_tieout_audit AS (
    SELECT
        validation_type,
        validation_sub_type,
        name,
        criteria,
        phase,
        validated_date,
        fiscal_year_month_int,
        total_amount,
        top_node_amt,
        diff_amount,
        diff_amt_pct
    FROM {{ source('raw', 'el_rma_tac_gl_tieout_audit') }}
),

source_el_cxpnl_rec_trend_audit AS (
    SELECT
        validation_type,
        validation_sub_type,
        name,
        criteria,
        phase,
        validated_date,
        fiscal_year_month_int,
        curr_count,
        avg_count,
        diff,
        tolerance_diff,
        diff_pct,
        final_result
    FROM {{ source('raw', 'el_cxpnl_rec_trend_audit') }}
),

source_el_cxpnl_attrib_ctrb_trnd_adt AS (
    SELECT
        validation_type,
        validation_sub_type,
        name,
        criteria,
        phase,
        validated_date,
        fiscal_year_month_int,
        attribute_type,
        curr_price,
        avg_price,
        difference,
        tolerance_diff,
        difference_pct,
        final_result
    FROM {{ source('raw', 'el_cxpnl_attrib_ctrb_trnd_adt') }}
),

final AS (
    SELECT
        validation_type,
        validation_sub_type,
        name,
        criteria,
        phase,
        validated_date,
        fiscal_year_month_int,
        direct_flag,
        as_tss_flag,
        cost_type,
        amount
    FROM source_el_cxpnl_attrib_ctrb_trnd_adt
)

SELECT * FROM final
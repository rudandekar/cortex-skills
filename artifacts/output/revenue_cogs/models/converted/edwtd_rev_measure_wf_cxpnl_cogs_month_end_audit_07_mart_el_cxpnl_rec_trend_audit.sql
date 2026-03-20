{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cxpnl_cogs_month_end_audit', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_CXPNL_COGS_MONTH_END_AUDIT',
        'target_table': 'EL_CXPNL_REC_TREND_AUDIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.671122+00:00'
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

source_el_acts_worktime_gl_validation AS (
    SELECT
        validation_type,
        validation_sub_type,
        name,
        criteria,
        phase,
        validated_date,
        fiscal_year_month_int,
        theater,
        delivery_channel,
        work_tine_or_tac_cost,
        sr_count
    FROM {{ source('raw', 'el_acts_worktime_gl_validation') }}
),

source_el_sr_rma_catchup_validation AS (
    SELECT
        validation_type,
        validation_sub_type,
        name,
        criteria,
        phase,
        validated_date,
        fiscal_year_month_int,
        catchup_mth_rec_count,
        curr_mth_rec_count,
        tolerance_diff,
        catchup_pct,
        final_result
    FROM {{ source('raw', 'el_sr_rma_catchup_validation') }}
),

source_el_excel_driver_gslo_audit AS (
    SELECT
        validation_type,
        validation_sub_type,
        name,
        criteria,
        phase,
        validated_date,
        fiscal_year_month_int,
        file_upload_flg,
        cur_mth_count,
        prev_mth_count,
        tolerance_diff,
        difference_pct,
        final_result
    FROM {{ source('raw', 'el_excel_driver_gslo_audit') }}
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

source_el_hmp_audit AS (
    SELECT
        validation_type,
        validation_sub_type,
        name,
        criteria,
        phase,
        validated_date,
        fiscal_year_month_int,
        wg_name,
        theater,
        delivery_channel
    FROM {{ source('raw', 'el_hmp_audit') }}
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
        curr_count,
        avg_count,
        diff,
        tolerance_diff,
        diff_pct,
        final_result
    FROM source_el_cxpnl_attrib_ctrb_trnd_adt
)

SELECT * FROM final
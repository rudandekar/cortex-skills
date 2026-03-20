{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cxpnl_cogs_erly_wrng_audit1', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_CXPNL_COGS_ERLY_WRNG_AUDIT1',
        'target_table': 'EL_HMP_AUDIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.100828+00:00'
    }
) }}

WITH 

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

final AS (
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
    FROM source_el_hmp_audit
)

SELECT * FROM final
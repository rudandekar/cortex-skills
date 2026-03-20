{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_order_lines_f_wtcalc', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_ORDER_LINES_F_WTCALC',
        'target_table': 'EL_TSS_ORDER_LINES_F_WTCALC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.988318+00:00'
    }
) }}

WITH 

source_st_tss_order_lines_f_wtcalc AS (
    SELECT
        line_id,
        bl_line_key,
        shipped_quantity,
        bl_order_key,
        shippable_flag,
        labor_line_flag,
        bl_last_update_date
    FROM {{ source('raw', 'st_tss_order_lines_f_wtcalc') }}
),

final AS (
    SELECT
        line_id,
        bl_line_key,
        shipped_quantity,
        bl_order_key,
        shippable_flag,
        labor_line_flag,
        bl_last_update_date,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_tss_order_lines_f_wtcalc
)

SELECT * FROM final
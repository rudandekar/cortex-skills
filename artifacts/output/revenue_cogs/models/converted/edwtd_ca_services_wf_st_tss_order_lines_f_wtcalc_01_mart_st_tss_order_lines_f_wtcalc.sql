{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_order_lines_f_wtcalc', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_ORDER_LINES_F_WTCALC',
        'target_table': 'ST_TSS_ORDER_LINES_F_WTCALC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.544825+00:00'
    }
) }}

WITH 

source_ff_tss_order_lines_f_wtcalc AS (
    SELECT
        line_id,
        bl_line_key,
        shipped_quantity,
        bl_order_key,
        shippable_flag,
        labor_line_flag,
        bl_last_update_date
    FROM {{ source('raw', 'ff_tss_order_lines_f_wtcalc') }}
),

transformed_ext_datatransform AS (
    SELECT
    line_id,
    bl_line_key,
    shipped_quantity,
    bl_order_key,
    shippable_flag,
    labor_line_flag,
    bl_last_update_date,
    IFF(LTRIM(RTRIM(LINE_ID))='\N',-999,TO_BIGINT(LINE_ID)) AS o_line_id,
    IFF(LTRIM(RTRIM(BL_LINE_KEY))='\N',-999,TO_BIGINT(BL_LINE_KEY)) AS o_bl_line_key,
    IFF(LTRIM(RTRIM(SHIPPED_QUANTITY)) = '\N',-999,TO_INTEGER(SHIPPED_QUANTITY)) AS o_shipped_quantity,
    IFF(LTRIM(RTRIM(BL_ORDER_KEY))='\N',-999,TO_BIGINT(BL_ORDER_KEY)) AS o_bl_order_key,
    IFF(LTRIM(RTRIM(SHIPPABLE_FLAG)) = '\N',NULL,SHIPPABLE_FLAG) AS o_shippable_flag,
    IFF(LTRIM(RTRIM(LABOR_LINE_FLAG)) = '\N',NULL,LABOR_LINE_FLAG) AS o_labor_line_flag,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATE_DATE)) = '\N',TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(BL_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_last_update_date
    FROM source_ff_tss_order_lines_f_wtcalc
),

final AS (
    SELECT
        line_id,
        bl_line_key,
        shipped_quantity,
        bl_order_key,
        shippable_flag,
        labor_line_flag,
        bl_last_update_date
    FROM transformed_ext_datatransform
)

SELECT * FROM final
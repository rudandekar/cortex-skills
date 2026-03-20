{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dcv_om_oe_price_adjustments', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_DCV_OM_OE_PRICE_ADJUSTMENTS',
        'target_table': 'ST_DCV_OM_OE_PRICE_ADJUSTMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.020857+00:00'
    }
) }}

WITH 

source_st_dcv_om_oe_price_adjustments AS (
    SELECT
        price_adjustment_id,
        attribute12,
        global_name
    FROM {{ source('raw', 'st_dcv_om_oe_price_adjustments') }}
),

final AS (
    SELECT
        price_adjustment_id,
        attribute12,
        global_name
    FROM source_st_dcv_om_oe_price_adjustments
)

SELECT * FROM final
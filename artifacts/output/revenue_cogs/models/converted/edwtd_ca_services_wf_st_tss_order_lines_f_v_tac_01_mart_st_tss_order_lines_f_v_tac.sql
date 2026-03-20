{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_order_lines_f_v', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_ORDER_LINES_F_V',
        'target_table': 'ST_TSS_ORDER_LINES_F_V_TAC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.460075+00:00'
    }
) }}

WITH 

source_ff_tss_order_lines_f_v AS (
    SELECT
        bl_line_key,
        std_cost_ext_usd_amt,
        shipped_quantity,
        gbl_list_price_ext_usd_amt,
        header_id,
        actual_ship_date_calendar_key,
        product_key,
        inventory_item_id,
        line_type_lkp_key,
        creation_date,
        last_update_date,
        bl_creation_date,
        bl_last_update_date,
        actual_ship_date,
        labor_line_flag
    FROM {{ source('raw', 'ff_tss_order_lines_f_v') }}
),

transformed_exptrans AS (
    SELECT
    bl_line_key,
    std_cost_ext_usd_amt,
    shipped_quantity,
    gbl_list_price_ext_usd_amt,
    header_id,
    actual_ship_date_calendar_key,
    product_key,
    inventory_item_id,
    line_type_lkp_key,
    creation_date,
    last_update_date,
    bl_creation_date,
    bl_last_update_date,
    actual_ship_date,
    labor_line_flag,
    IFF(LTRIM(RTRIM(BL_LINE_KEY)) = '\N',NULL,TO_INTEGER(BL_LINE_KEY)) AS o_bl_line_key,
    IFF(LTRIM(RTRIM(STD_COST_EXT_USD_AMT,5)) = '\N',NULL,TO_DECIMAL(STD_COST_EXT_USD_AMT,5)) AS o_std_cost_ext_usd_amt,
    IFF(LTRIM(RTRIM(SHIPPED_QUANTITY)) = '\N',NULL,TO_BIGINT(SHIPPED_QUANTITY)) AS o_shipped_quantity,
    IFF(LTRIM(RTRIM(GBL_LIST_PRICE_EXT_USD_AMT,5)) = '\N',NULL,TO_DECIMAL(GBL_LIST_PRICE_EXT_USD_AMT,5)) AS o_gbl_list_price_ext_usd_amt,
    IFF(LTRIM(RTRIM(HEADER_ID)) = '\N',NULL,TO_BIGINT(HEADER_ID)) AS o_header_id,
    IFF(LTRIM(RTRIM(ACTUAL_SHIP_DATE_CALENDAR_KEY)) = '\N',NULL,TO_BIGINT(ACTUAL_SHIP_DATE_CALENDAR_KEY)) AS o_actual_ship_date_calendar_key,
    IFF(LTRIM(RTRIM(PRODUCT_KEY)) = '\N',NULL,TO_BIGINT(PRODUCT_KEY)) AS o_product_key,
    IFF(LTRIM(RTRIM(INVENTORY_ITEM_ID)) = '\N',NULL,TO_BIGINT(INVENTORY_ITEM_ID)) AS o_inventory_item_id,
    IFF(LTRIM(RTRIM(LINE_TYPE_LKP_KEY)) = '\N',NULL,TO_BIGINT(LINE_TYPE_LKP_KEY)) AS o_line_type_lkp_key,
    IFF(LTRIM(RTRIM(CREATION_DATE)) = '\N',NULL,TO_DATE(CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_creation_date,
    IFF(LTRIM(RTRIM(LAST_UPDATE_DATE)) = '\N',NULL,TO_DATE(LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_last_update_date,
    IFF(LTRIM(RTRIM(BL_CREATION_DATE)) = '\N',NULL,TO_DATE(BL_CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_creation_date,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATE_DATE)) = '\N',NULL,TO_DATE(BL_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_last_update_date,
    IFF(LTRIM(RTRIM(ACTUAL_SHIP_DATE)) = '\N',NULL,TO_DATE(ACTUAL_SHIP_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_actual_ship_date,
    IFF(LTRIM(RTRIM(LABOR_LINE_FLAG)) = '\N',NULL,TO_CHAR(LABOR_LINE_FLAG)) AS o_labor_line_flag
    FROM source_ff_tss_order_lines_f_v
),

final AS (
    SELECT
        bl_line_key,
        std_cost_ext_usd_amt,
        shipped_quantity,
        gbl_list_price_ext_usd_amt,
        header_id,
        actual_ship_date_calendar_key,
        product_key,
        inventory_item_id,
        line_type_lkp_key,
        creation_date,
        last_update_date,
        bl_creation_date,
        bl_last_update_date,
        actual_ship_date,
        labor_line_flag
    FROM transformed_exptrans
)

SELECT * FROM final
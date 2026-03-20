{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_gpf_bts_gp_tan_mpg_forecast', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_GPF_BTS_GP_TAN_MPG_FORECAST',
        'target_table': 'EX_XXCMF_SP_BTS_EXT_PIDTAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.769197+00:00'
    }
) }}

WITH 

source_st_xxcmf_sp_bts_ext_pidtan AS (
    SELECT
        tan_id,
        tan_desc,
        tan_organization_code,
        pid,
        pid_desc,
        pid_organization_code,
        bucket_date,
        sales_order,
        unconsumed_forecast,
        released_po,
        planned_po,
        publish_date,
        creation_date,
        created_by,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_xxcmf_sp_bts_ext_pidtan') }}
),

source_w_gpf_bts_gp_tan_mpg_forecast AS (
    SELECT
        tan_inventory_orgn_name_key,
        goods_product_key,
        tan_item_key,
        bk_frcst_fiscal_week_num_int,
        bk_frcst_fiscal_year_num_int,
        bk_frcst_fiscal_calendar_cd,
        bk_forecast_publication_dt,
        product_inventory_orgn_key,
        committed_tan_supply_qty,
        unconsumed_tan_forecast_qty,
        planned_tan_supply_qty,
        product_ordered_qty,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_gpf_bts_gp_tan_mpg_forecast') }}
),

final AS (
    SELECT
        tan_id,
        tan_desc,
        tan_organization_code,
        pid,
        pid_desc,
        pid_organization_code,
        bucket_date,
        sales_order,
        unconsumed_forecast,
        released_po,
        planned_po,
        publish_date,
        creation_date,
        created_by,
        batch_id,
        create_datetime,
        action_code
    FROM source_w_gpf_bts_gp_tan_mpg_forecast
)

SELECT * FROM final
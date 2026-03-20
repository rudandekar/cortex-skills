{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_build_and_ship_plan', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_N_BUILD_AND_SHIP_PLAN',
        'target_table': 'N_BUILD_AND_SHIP_PLAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.523350+00:00'
    }
) }}

WITH 

source_w_build_and_ship_plan AS (
    SELECT
        bk_product_family_id,
        bk_fiscal_week_num_int,
        bk_fiscal_year_num_int,
        bk_fiscal_calendar_cd,
        bk_sales_channel_src_type,
        bk_sales_channel_cd,
        bk_delivery_type_cd,
        build_plan_usd_amt,
        ship_plan_usd_amt,
        bk_plan_publish_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_build_and_ship_plan') }}
),

final AS (
    SELECT
        bk_product_family_id,
        bk_fiscal_week_num_int,
        bk_fiscal_year_num_int,
        bk_fiscal_calendar_cd,
        bk_sales_channel_src_type,
        bk_sales_channel_cd,
        bk_delivery_type_cd,
        build_plan_usd_amt,
        ship_plan_usd_amt,
        bk_plan_publish_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_build_and_ship_plan
)

SELECT * FROM final
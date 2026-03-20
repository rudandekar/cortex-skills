{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_gross_bookings_plan', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_N_GROSS_BOOKINGS_PLAN',
        'target_table': 'N_GROSS_BOOKINGS_PLAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.556767+00:00'
    }
) }}

WITH 

source_w_gross_bookings_plan AS (
    SELECT
        bk_product_family_id,
        bk_fiscal_week_num_int,
        bk_fiscal_year_num_int,
        bk_fiscal_calendar_cd,
        bk_sales_channel_src_type,
        bk_sales_channel_cd,
        physical_gross_plan_usd_amt,
        total_gross_plan_usd_amt,
        bk_plan_publish_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_gross_bookings_plan') }}
),

final AS (
    SELECT
        bk_product_family_id,
        bk_fiscal_week_num_int,
        bk_fiscal_year_num_int,
        bk_fiscal_calendar_cd,
        bk_sales_channel_src_type,
        bk_sales_channel_cd,
        physical_gross_plan_usd_amt,
        total_gross_plan_usd_amt,
        bk_plan_publish_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_gross_bookings_plan
)

SELECT * FROM final
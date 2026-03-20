{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_net_bookings_plan', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_W_NET_BOOKINGS_PLAN',
        'target_table': 'W_NET_BOOKINGS_PLAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.263036+00:00'
    }
) }}

WITH 

source_st_xxcmf_sp_crp_net_bookings AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        sales_channel,
        net_bookings_total,
        creation_date,
        id
    FROM {{ source('raw', 'st_xxcmf_sp_crp_net_bookings') }}
),

final AS (
    SELECT
        bk_product_family_id,
        bk_fiscal_week_num_int,
        bk_fiscal_year_num_int,
        bk_fiscal_calendar_cd,
        bk_sales_channel_src_type,
        bk_sales_channel_cd,
        bk_published_dt,
        net_bookings_plan_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_xxcmf_sp_crp_net_bookings
)

SELECT * FROM final
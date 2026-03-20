{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_fcm_deal_mapping', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_W_FCM_DEAL_MAPPING',
        'target_table': 'W_FCM_DEAL_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.157604+00:00'
    }
) }}

WITH 

source_w_fcm_deal_mapping AS (
    SELECT
        bk_deal_id,
        bk_dv_fiscal_year_mth_num_int,
        src_reported_fcm_customer_name,
        major_offer_type_cd,
        deal_start_dtm,
        dv_deal_start_dt,
        deal_end_dtm,
        dv_deal_end_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        xaas_flg,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_fcm_deal_mapping') }}
),

final AS (
    SELECT
        bk_deal_id,
        bk_dv_fiscal_year_mth_num_int,
        src_reported_fcm_customer_name,
        major_offer_type_cd,
        deal_start_dtm,
        dv_deal_start_dt,
        deal_end_dtm,
        dv_deal_end_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        xaas_flg,
        action_code,
        dml_type
    FROM source_w_fcm_deal_mapping
)

SELECT * FROM final
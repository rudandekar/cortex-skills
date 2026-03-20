{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_fcm_deal_mapping', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_FF_FCM_DEAL_MAPPING',
        'target_table': 'FF_FCM_DEAL_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.652820+00:00'
    }
) }}

WITH 

source_fcm_deal_mapping AS (
    SELECT
        bk_deal_id,
        src_reported_fcm_customer_name,
        major_offer_type_cd,
        deal_start_dtm,
        dv_deal_start_dt,
        deal_end_dtm,
        dv_deal_end_dt,
        status_dtm,
        xaas_flag
    FROM {{ source('raw', 'fcm_deal_mapping') }}
),

transformed_exp_ff_fcm_deal_mapping AS (
    SELECT
    bk_deal_id,
    src_reported_fcm_customer_name,
    major_offer_type_cd,
    deal_start_dtm,
    dv_deal_start_dt,
    deal_end_dtm,
    dv_deal_end_dt,
    status_dtm,
    xaas_flag,
    'I' AS action_code,
    CURRENT_TIMESTAMP() AS create_datetime
    FROM source_fcm_deal_mapping
),

final AS (
    SELECT
        bk_deal_id,
        src_reported_fcm_customer_name,
        major_offer_type_cd,
        deal_start_dtm,
        dv_deal_start_dt,
        deal_end_dtm,
        dv_deal_end_dt,
        status_dtm,
        action_code,
        create_datetime,
        xaas_flag
    FROM transformed_exp_ff_fcm_deal_mapping
)

SELECT * FROM final
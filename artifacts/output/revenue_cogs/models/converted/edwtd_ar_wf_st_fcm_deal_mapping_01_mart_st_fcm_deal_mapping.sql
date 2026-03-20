{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_fcm_deal_mapping', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_FCM_DEAL_MAPPING',
        'target_table': 'ST_FCM_DEAL_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.882826+00:00'
    }
) }}

WITH 

source_ff_fcm_deal_mapping AS (
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
    FROM {{ source('raw', 'ff_fcm_deal_mapping') }}
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
    FROM source_ff_fcm_deal_mapping
)

SELECT * FROM final
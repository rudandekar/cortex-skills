{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_quali_form_response', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_QUALI_FORM_RESPONSE',
        'target_table': 'W_DEAL_QUALI_FORM_RESPONSE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.722347+00:00'
    }
) }}

WITH 

source_w_deal_quali_form_response AS (
    SELECT
        bk_deal_id,
        bk_rebate_question_id_int,
        bk_question_response_txt,
        src_crtd_csco_wrkr_prty_key,
        src_created_dtm,
        dv_src_created_dt,
        src_updtd_csco_wrkr_prty_key,
        src_updated_dtm,
        dv_srs_updated_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_quali_form_response') }}
),

final AS (
    SELECT
        bk_deal_id,
        bk_rebate_question_id_int,
        bk_question_response_txt,
        src_crtd_csco_wrkr_prty_key,
        src_created_dtm,
        dv_src_created_dt,
        src_updtd_csco_wrkr_prty_key,
        src_updated_dtm,
        dv_srs_updated_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_deal_quali_form_response
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_resubmited_deal', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_RESUBMITED_DEAL',
        'target_table': 'W_RESUBMITED_DEAL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.876787+00:00'
    }
) }}

WITH 

source_st_cq_deal_submit_h AS (
    SELECT
        batch_id,
        deal_object_id,
        opty_number,
        submit_date,
        submit_number,
        submit_by,
        appr_route_history,
        reason_for_approval,
        product_family,
        sotd_reason,
        comments,
        pricing_band,
        final_band,
        deal_approval,
        create_datetime,
        action_code,
        ss_code
    FROM {{ source('raw', 'st_cq_deal_submit_h') }}
),

source_wi_cq_deal_submit_h AS (
    SELECT
        batch_id,
        deal_object_id,
        opty_number,
        submit_date,
        submit_number,
        submit_by,
        appr_route_history,
        reason_for_approval,
        product_family,
        sotd_reason,
        comments,
        pricing_band,
        final_band,
        deal_approval,
        create_datetime,
        action_code,
        ss_code
    FROM {{ source('raw', 'wi_cq_deal_submit_h') }}
),

source_w_resubmited_deal AS (
    SELECT
        bk_deal_id,
        bk_submit_num_int,
        deal_submit_dt,
        submitted_by_csco_wrkr_pty_key,
        approval_route_hist_txt,
        reason_for_approval_txt,
        resubmission_reason_txt,
        product_family_desc,
        pricing_band_threshold_desc,
        final_band_desc,
        resubmitted_deal_comments_txt,
        last_submitted_record_flg,
        deal_aprvl_sbmsn_hist_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        final_band_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_resubmited_deal') }}
),

final AS (
    SELECT
        bk_deal_id,
        bk_submit_num_int,
        deal_submit_dt,
        submitted_by_csco_wrkr_pty_key,
        approval_route_hist_txt,
        reason_for_approval_txt,
        resubmission_reason_txt,
        product_family_desc,
        pricing_band_threshold_desc,
        final_band_desc,
        resubmitted_deal_comments_txt,
        last_submitted_record_flg,
        deal_aprvl_sbmsn_hist_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        final_band_cd,
        action_code,
        dml_type
    FROM source_w_resubmited_deal
)

SELECT * FROM final
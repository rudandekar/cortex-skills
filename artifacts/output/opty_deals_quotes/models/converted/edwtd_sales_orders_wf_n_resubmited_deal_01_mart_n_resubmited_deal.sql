{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_resubmited_deal', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_RESUBMITED_DEAL',
        'target_table': 'N_RESUBMITED_DEAL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.955489+00:00'
    }
) }}

WITH 

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
        final_band_cd
    FROM source_w_resubmited_deal
)

SELECT * FROM final
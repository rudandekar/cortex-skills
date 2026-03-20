{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_promotion', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_PROMOTION',
        'target_table': 'N_DEAL_PROMOTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.910002+00:00'
    }
) }}

WITH 

source_w_deal_promotion AS (
    SELECT
        bk_promotion_cd,
        bk_promotion_revision_num_int,
        bk_deal_id,
        created_dtm,
        dv_created_dt,
        last_updated_dtm,
        dv_last_updated_dt,
        src_rptd_updated_by_name,
        src_rptd_created_by_name,
        allow_promotion_change_flg,
        is_bundle_promotion_flg,
        channel_program_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_quote_num,
        sk_reward_id_int,
        reward_name,
        reward_is_selectable_flg,
        reward_is_selected_flg,
        reward_applied_version_num,
        reward_is_approved_flg,
        action_code,
        dml_type,
        reward_auto_approved_flg,
        reward_status_name
    FROM {{ source('raw', 'w_deal_promotion') }}
),

final AS (
    SELECT
        bk_promotion_cd,
        bk_promotion_revision_num_int,
        bk_deal_id,
        created_dtm,
        dv_created_dt,
        last_updated_dtm,
        dv_last_updated_dt,
        src_rptd_updated_by_name,
        src_rptd_created_by_name,
        allow_promotion_change_flg,
        is_bundle_promotion_flg,
        channel_program_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_quote_num,
        sk_reward_id_int,
        reward_name,
        reward_is_selectable_flg,
        reward_is_selected_flg,
        reward_applied_version_num,
        reward_is_approved_flg,
        reward_auto_approved_flg,
        reward_status_name
    FROM source_w_deal_promotion
)

SELECT * FROM final
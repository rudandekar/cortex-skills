{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_sales_order_line_hold', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_SALES_ORDER_LINE_HOLD',
        'target_table': 'N_CCRM_SALES_ORDER_LINE_HOLD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.188154+00:00'
    }
) }}

WITH 

source_w_ccrm_sales_order_line_hold AS (
    SELECT
        bk_ccrm_adjustment_grp_id_int,
        bk_adjustment_category_cd,
        bk_ccrm_adjustment_reason_cd,
        bk_ccrm_adj_reason_ver_num_int,
        bk_accounting_scope_cd,
        sales_order_line_key,
        rae_adjustment_pct,
        invoice_hold_flg,
        split_accounting_scope_flg,
        rsn_cd_defer_immdtly_flg,
        template_adjustment_name,
        user_specified_rsn_action_cd,
        accounting_scope_hold_pct,
        rsn_cd_starting_point_num_int,
        rsn_cd_ranking_num_int,
        rsn_cd_priority_num_int,
        total_unrecognized_pct,
        total_sequential_prvsn_pct,
        total_sequential_dfrrd_pct,
        actual_reclass_pct,
        actual_usage_pct,
        actual_released_pct,
        actual_unrecognized_pct,
        requested_unrecognized_pct,
        source_created_dtm,
        sk_hold_detail_id_int,
        src_rptd_vsoe_accntng_scp_cd,
        rae_adjustment_key,
        ccrm_profile_id_int,
        ccrm_adjustment_subrsn_cd,
        ccrm_adjust_subrsn_ver_num_int,
        ccrm_element_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_sales_order_line_hold') }}
),

final AS (
    SELECT
        bk_ccrm_adjustment_grp_id_int,
        bk_adjustment_category_cd,
        bk_ccrm_adjustment_reason_cd,
        bk_ccrm_adj_reason_ver_num_int,
        bk_accounting_scope_cd,
        sales_order_line_key,
        rae_adjustment_pct,
        invoice_hold_flg,
        split_accounting_scope_flg,
        rsn_cd_defer_immdtly_flg,
        template_adjustment_name,
        user_specified_rsn_action_cd,
        accounting_scope_hold_pct,
        rsn_cd_starting_point_num_int,
        rsn_cd_ranking_num_int,
        rsn_cd_priority_num_int,
        total_unrecognized_pct,
        total_sequential_prvsn_pct,
        total_sequential_dfrrd_pct,
        actual_reclass_pct,
        actual_usage_pct,
        actual_released_pct,
        actual_unrecognized_pct,
        requested_unrecognized_pct,
        source_created_dtm,
        sk_hold_detail_id_int,
        src_rptd_vsoe_accntng_scp_cd,
        rae_adjustment_key,
        ccrm_profile_id_int,
        ccrm_adjustment_subrsn_cd,
        ccrm_adjust_subrsn_ver_num_int,
        ccrm_element_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ccrm_sales_order_line_hold
)

SELECT * FROM final
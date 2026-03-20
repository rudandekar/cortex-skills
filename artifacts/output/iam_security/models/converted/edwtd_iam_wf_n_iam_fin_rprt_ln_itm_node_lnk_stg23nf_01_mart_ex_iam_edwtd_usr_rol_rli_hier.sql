{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_fin_rprt_ln_itm_node_lnk_stg23nf', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_FIN_RPRT_LN_ITM_NODE_LNK_STG23NF',
        'target_table': 'EX_IAM_EDWTD_USR_ROL_RLI_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.744345+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_usr_rol_rli_hier AS (
    SELECT
        batch_id,
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        data_struct_type,
        lvl_number,
        erp_segment1,
        assignment_type,
        data_res_expt_flag,
        excl_restr_flag,
        restriction_flag,
        proxy_flag,
        grantor_universal_id,
        grantor_cec_id,
        grantor_cco_id,
        status,
        tr_flag,
        last_action,
        created_by,
        create_date,
        updated_by,
        update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_iam_edwtd_usr_rol_rli_hier') }}
),

update_strategy_upd_ins_iam_fin_rprt_ln_itm_node_lnk AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM source_st_iam_edwtd_usr_rol_rli_hier
    WHERE DD_INSERT != 3
),

transformed_exp_iam_fin_rprt_ln_itm_node_lnk_ins AS (
    SELECT
    bk_iam_role_name,
    iam_user_key,
    iam_application_key,
    bk_finance_report_line_item_cd,
    standard_or_exception_flg,
    assignment_type_cd,
    exclusive_or_restrictive_flg,
    source_deleted_flg,
    iam_level_num_int
    FROM update_strategy_upd_ins_iam_fin_rprt_ln_itm_node_lnk
),

transformed_exp_iam_fin_rprt_ln_itm_node_lnk_upd1 AS (
    SELECT
    bk_iam_role_name,
    iam_user_key,
    iam_application_key,
    bk_finance_report_line_item_cd,
    standard_or_exception_flg,
    assignment_type_cd,
    exclusive_or_restrictive_flg,
    source_deleted_flg,
    iam_level_num_int,
    edw_update_dtm
    FROM transformed_exp_iam_fin_rprt_ln_itm_node_lnk_ins
),

update_strategy_upd_upd1_iam_fin_rprt_ln_itm_node_lnk AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_iam_fin_rprt_ln_itm_node_lnk_upd1
    WHERE DD_UPDATE != 3
),

transformed_exp_iam_fin_rprt_ln_itm_node_lnk_upd2 AS (
    SELECT
    bk_iam_role_name,
    iam_user_key,
    iam_application_key,
    bk_finance_report_line_item_cd,
    source_deleted_flg,
    edw_update_dtm
    FROM update_strategy_upd_upd1_iam_fin_rprt_ln_itm_node_lnk
),

update_strategy_upd_upd2_iam_fin_rprt_ln_itm_node_lnk AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_iam_fin_rprt_ln_itm_node_lnk_upd2
    WHERE DD_UPDATE != 3
),

final AS (
    SELECT
        batch_id,
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        data_struct_type,
        lvl_number,
        erp_segment1,
        assignment_type,
        data_res_expt_flag,
        excl_restr_flag,
        restriction_flag,
        proxy_flag,
        grantor_universal_id,
        grantor_cec_id,
        grantor_cco_id,
        status,
        tr_flag,
        last_action,
        created_by,
        create_date,
        updated_by,
        update_date,
        create_datetime,
        action_code,
        exception_type
    FROM update_strategy_upd_upd2_iam_fin_rprt_ln_itm_node_lnk
)

SELECT * FROM final
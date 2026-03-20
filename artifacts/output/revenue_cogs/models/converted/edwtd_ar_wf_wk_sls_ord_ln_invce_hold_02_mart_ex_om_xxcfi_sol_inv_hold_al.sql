{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_sls_ord_ln_invce_hold', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WK_SLS_ORD_LN_INVCE_HOLD',
        'target_table': 'EX_OM_XXCFI_SOL_INV_HOLD_AL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.827392+00:00'
    }
) }}

WITH 

source_sm_sales_order_line AS (
    SELECT
        sales_order_line_key,
        ss_code,
        sk_so_line_id_int,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_sales_order_line') }}
),

source_st_om_xxcfi_sol_inv_hold_al AS (
    SELECT
        batch_id,
        so_line_id,
        invoice_hold_flg,
        global_name,
        db_source_name,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_xxcfi_sol_inv_hold_al') }}
),

transformed_exp_ex_om_xxcfi_sol_inv_hold_al AS (
    SELECT
    batch_id,
    so_line_id,
    invoice_hold_flg,
    global_name,
    db_source_name,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    ges_update_date,
    create_datetime,
    action_code,
    'RI' AS exception_type
    FROM source_st_om_xxcfi_sol_inv_hold_al
),

transformed_exp_w_sls_ord_ln_invce_hold AS (
    SELECT
    sales_order_line_key,
    invoice_hold_flg,
    edw_create_dtm,
    edw_create_user,
    edw_update_dtm,
    edw_update_user,
    start_tv_dt,
    end_tv_dt,
    action_code,
    rank_index,
    dml_type
    FROM transformed_exp_ex_om_xxcfi_sol_inv_hold_al
),

transformed_exp_ex_cg_xxcfir_sol_inv_hold_al1 AS (
    SELECT
    batch_id,
    so_line_id,
    inv_hold,
    global_name,
    db_source_name,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    create_datetime,
    action_code,
    org_id,
    'RI' AS exception_type
    FROM transformed_exp_w_sls_ord_ln_invce_hold
),

final AS (
    SELECT
        batch_id,
        so_line_id,
        invoice_hold_flg,
        global_name,
        db_source_name,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        ges_update_date,
        create_datetime,
        action_code,
        exception_type
    FROM transformed_exp_ex_cg_xxcfir_sol_inv_hold_al1
)

SELECT * FROM final
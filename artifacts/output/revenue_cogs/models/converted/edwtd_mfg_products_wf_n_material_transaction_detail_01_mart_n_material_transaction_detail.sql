{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_material_transaction_detail', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_MATERIAL_TRANSACTION_DETAIL',
        'target_table': 'N_MATERIAL_TRANSACTION_DETAIL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.605032+00:00'
    }
) }}

WITH 

source_w_material_transaction_detail AS (
    SELECT
        material_trnsctn_detail_key,
        cost_element_cd,
        accounting_line_type_cd,
        cr_dr_type,
        unit_usd_amt,
        transaction_detail_qty,
        bk_material_transaction_key,
        bk_general_ledger_account_key,
        set_of_books_key,
        ss_cd,
        ru_dr_usd_amt,
        ru_cr_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        gl_posted_flg,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_material_transaction_detail') }}
),

final AS (
    SELECT
        material_trnsctn_detail_key,
        cost_element_cd,
        accounting_line_type_cd,
        cr_dr_type,
        unit_usd_amt,
        transaction_detail_qty,
        bk_material_transaction_key,
        bk_general_ledger_account_key,
        set_of_books_key,
        ss_cd,
        ru_dr_usd_amt,
        ru_cr_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        gl_posted_flg
    FROM source_w_material_transaction_detail
)

SELECT * FROM final
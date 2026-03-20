{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_material_transaction_detail', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_MATERIAL_TRANSACTION_DETAIL',
        'target_table': 'W_MATERIAL_TRANSACTION_DETAIL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.784763+00:00'
    }
) }}

WITH 

source_st_cg1_mtl_transaction_acc AS (
    SELECT
        transaction_id,
        reference_account,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        inventory_item_id,
        organization_id,
        transaction_date,
        transaction_source_id,
        transaction_source_type_id,
        transaction_value,
        primary_quantity,
        gl_batch_id,
        accounting_line_type,
        base_transaction_value,
        contra_set_id,
        rate_or_amount,
        basis_type,
        resource_id,
        cost_element_id,
        activity_id,
        currency_code,
        currency_conversion_date,
        currency_conversion_type,
        currency_conversion_rate,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        encumbrance_type_id,
        repetitive_schedule_id,
        gl_sl_link_id,
        ussgl_transaction_code,
        inv_sub_ledger_id,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        ss_cd
    FROM {{ source('raw', 'st_cg1_mtl_transaction_acc') }}
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
        journal_entry_batch_id_int,
        set_of_books_key,
        ss_cd,
        ru_dr_usd_amt,
        ru_cr_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type,
        gl_posted_flg
    FROM source_st_cg1_mtl_transaction_acc
)

SELECT * FROM final
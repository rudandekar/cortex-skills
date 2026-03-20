{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_po_req_dist_all_dels', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_PO_REQ_DIST_ALL_DELS',
        'target_table': 'EL_PO_REQ_DIST_ALL_DELS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.779372+00:00'
    }
) }}

WITH 

source_st_mf_po_req_dist_all_dels AS (
    SELECT
        batch_id,
        accrual_account_id,
        allocation_type,
        allocation_value,
        attribute_category,
        attribute1,
        attribute10,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        budget_account_id,
        code_combination_id,
        created_by,
        creation_date,
        distribution_id,
        distribution_num,
        encumbered_flag,
        last_update_date,
        last_updated_by,
        nonrecoverable_tax,
        org_id,
        project_accounting_context,
        project_related_flag,
        recoverable_tax,
        recovery_rate,
        req_line_quantity,
        request_id,
        requisition_line_id,
        set_of_books_id,
        source_req_distribution_id,
        tax_recovery_override_flag,
        variance_account_id,
        ges_delete_date,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_po_req_dist_all_dels') }}
),

final AS (
    SELECT
        code_combination_id,
        creation_date,
        distribution_id,
        distribution_num,
        last_update_date,
        org_id,
        requisition_line_id,
        set_of_books_id,
        source_req_distribution_id,
        ges_delete_date,
        global_name
    FROM source_st_mf_po_req_dist_all_dels
)

SELECT * FROM final
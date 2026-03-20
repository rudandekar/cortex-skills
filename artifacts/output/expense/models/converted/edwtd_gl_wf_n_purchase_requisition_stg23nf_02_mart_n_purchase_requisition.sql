{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_purchase_requisition_stg23nf', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_PURCHASE_REQUISITION_STG23NF',
        'target_table': 'N_PURCHASE_REQUISITION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.102076+00:00'
    }
) }}

WITH 

source_st_cg1_po_requisition_heade AS (
    SELECT
        requisition_header_id,
        preparer_id,
        last_update_date,
        last_updated_by,
        segment1,
        summary_flag,
        enabled_flag,
        segment2,
        segment3,
        segment4,
        segment5,
        start_date_active,
        end_date_active,
        last_update_login,
        creation_date,
        created_by,
        description,
        authorization_status,
        note_to_authorizer,
        type_lookup_code,
        transferred_to_oe_flag,
        attribute_category,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        on_line_flag,
        preliminary_research_flag,
        research_complete_flag,
        preparer_finished_flag,
        preparer_finished_date,
        agent_return_flag,
        agent_return_note,
        cancel_flag,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        ussgl_transaction_code,
        government_context,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        interface_source_code,
        interface_source_line_id,
        closed_code,
        org_id,
        wf_item_type,
        wf_item_key,
        emergency_po_num,
        pcard_id,
        apps_source_code,
        cbc_accounting_date,
        change_pending_flag,
        active_shopping_cart_flag,
        contractor_status,
        contractor_requisition_flag,
        supplier_notified_flag,
        emergency_po_org_id,
        approved_date,
        tax_attribute_update_code,
        first_approver_id,
        first_position_id,
        federal_flag,
        conformed_header_id,
        revision_num,
        amendment_type,
        amendment_status,
        amendment_reason,
        uda_template_id,
        uda_template_date,
        clm_issuing_office,
        clm_cotr_office,
        clm_cotr_contact,
        clm_priority_code,
        suggested_award_no,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg1_po_requisition_heade') }}
),

source_ex_po_requisition_header AS (
    SELECT
        batch_id,
        apps_source_code,
        authorization_status,
        cancel_flag,
        closed_code,
        created_by,
        creation_date,
        description,
        enabled_flag,
        last_update_date,
        last_updated_by,
        org_id,
        preparer_id,
        program_update_date,
        request_id,
        requisition_header_id,
        segment1,
        type_lookup_code,
        ges_update_date,
        global_name,
        create_datetime,
        action_code,
        exception_type,
        attribute12
    FROM {{ source('raw', 'ex_po_requisition_header') }}
),

source_st_mf_po_requisition_header AS (
    SELECT
        batch_id,
        apps_source_code,
        attribute1,
        attribute10,
        attribute11,
        attribute2,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        authorization_status,
        cancel_flag,
        change_pending_flag,
        closed_code,
        created_by,
        creation_date,
        description,
        enabled_flag,
        interface_source_code,
        interface_source_line_id,
        last_update_date,
        last_updated_by,
        note_to_authorizer,
        org_id,
        preparer_id,
        program_update_date,
        request_id,
        requisition_header_id,
        segment1,
        summary_flag,
        transferred_to_oe_flag,
        type_lookup_code,
        ges_update_date,
        global_name,
        create_datetime,
        action_code,
        attribute12
    FROM {{ source('raw', 'st_mf_po_requisition_header') }}
),

transformed_exptrans2 AS (
    SELECT
    batch_id,
    apps_source_code,
    authorization_status,
    cancel_flag,
    closed_code,
    created_by,
    creation_date,
    description,
    enabled_flag,
    last_update_date,
    last_updated_by,
    org_id,
    preparer_id,
    program_update_date,
    request_id,
    requisition_header_id,
    segment1,
    type_lookup_code,
    ges_update_date,
    global_name,
    create_datetime,
    action_code,
    exception_type,
    interface_source_line_id,
    attribute12,
    interface_source_code
    FROM source_st_mf_po_requisition_header
),

transformed_exptrans AS (
    SELECT
    segment1,
    program_update_date,
    authorization_status,
    creation_date,
    approver_cisco_wrkr_party_key,
    preparer_cisco_wrkr_party_key,
    operating_unit_name_cd,
    source_deleted_flag,
    requisition_header_id,
    source_system_code,
    action_code,
    description,
    ru_sales_order_line_key,
    requisition_classification_cd,
    requisition_classification_id,
    interface_source_code
    FROM transformed_exptrans2
),

update_strategy_upd_ins AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exptrans
    WHERE DD_INSERT != 3
),

update_strategy_upd_upd AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_upd_ins
    WHERE DD_UPDATE != 3
),

transformed_exptrans1 AS (
    SELECT
    segment1,
    program_update_date,
    authorization_status,
    creation_date,
    approver_cisco_wrkr_party_key,
    preparer_cisco_wrkr_party_key,
    operating_unit_name_cd,
    source_deleted_flag,
    requisition_header_id,
    source_system_code,
    action_code,
    description,
    ru_sales_order_line_key,
    requisition_classification_cd,
    requisition_classification_id,
    interface_source_cd,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM update_strategy_upd_upd
),

update_strategy_excep_del AS (
    SELECT
        *,
        CASE 
            WHEN DD_DELETE = 0 THEN 'INSERT'
            WHEN DD_DELETE = 1 THEN 'UPDATE'
            WHEN DD_DELETE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exptrans1
    WHERE DD_DELETE != 3
),

update_strategy_upd_cg1_exp_del AS (
    SELECT
        *,
        CASE 
            WHEN DD_DELETE = 0 THEN 'INSERT'
            WHEN DD_DELETE = 1 THEN 'UPDATE'
            WHEN DD_DELETE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_excep_del
    WHERE DD_DELETE != 3
),

transformed_exptrans3 AS (
    SELECT
    requisition_header_id,
    preparer_id,
    last_update_date,
    last_updated_by,
    segment1,
    summary_flag,
    enabled_flag,
    segment2,
    segment3,
    segment4,
    segment5,
    start_date_active,
    end_date_active,
    last_update_login,
    creation_date,
    created_by,
    description,
    authorization_status,
    note_to_authorizer,
    type_lookup_code,
    transferred_to_oe_flag,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    on_line_flag,
    preliminary_research_flag,
    research_complete_flag,
    preparer_finished_flag,
    preparer_finished_date,
    agent_return_flag,
    agent_return_note,
    cancel_flag,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    ussgl_transaction_code,
    government_context,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    interface_source_code,
    interface_source_line_id,
    closed_code,
    org_id,
    wf_item_type,
    wf_item_key,
    emergency_po_num,
    pcard_id,
    apps_source_code,
    cbc_accounting_date,
    change_pending_flag,
    active_shopping_cart_flag,
    contractor_status,
    contractor_requisition_flag,
    supplier_notified_flag,
    emergency_po_org_id,
    approved_date,
    tax_attribute_update_code,
    first_approver_id,
    first_position_id,
    federal_flag,
    conformed_header_id,
    revision_num,
    amendment_type,
    amendment_status,
    amendment_reason,
    uda_template_id,
    uda_template_date,
    clm_issuing_office,
    clm_cotr_office,
    clm_cotr_contact,
    clm_priority_code,
    suggested_award_no,
    ges_update_date,
    global_name,
    batch_id,
    create_datetime,
    action_code,
    exception_type
    FROM update_strategy_upd_cg1_exp_del
),

final AS (
    SELECT
        bk_purchase_requisition_num,
        sol_role,
        pr_approved_dtm,
        pr_status_cd,
        pr_submit_dtm,
        approver_cisco_wrkr_party_key,
        preparer_cisco_wrkr_party_key,
        operating_unit_name_cd,
        source_deleted_flg,
        sk_purchase_requisition_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        purchase_requisition_descr,
        ru_sales_order_line_key,
        requisition_classification_cd,
        requisition_classification_id,
        interface_source_cd
    FROM transformed_exptrans3
)

SELECT * FROM final
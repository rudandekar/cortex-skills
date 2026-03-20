{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_co_snprefix', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_CO_SNPREFIX',
        'target_table': 'FF_CO_SNPREFIX_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.175174+00:00'
    }
) }}

WITH 

source_co_project AS (
    SELECT
        project_id,
        name,
        description,
        status,
        pid_assign_target_date,
        edcs_url,
        taa,
        wf_revision_id,
        rule,
        rule_assigned_by,
        rule_assignment_type,
        proj_assigned_to,
        proj_assigned_by,
        proj_assigned_date,
        comments,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        product_family,
        rule_assigned_date,
        sp_co_req,
        sp_co_req_details,
        dh_type,
        dr_reason,
        wizard_required,
        dt_email_sent_date,
        dt_revision_id,
        pid_info,
        inactive_reason,
        process,
        project_status,
        oemodm_taa_flag,
        source,
        wb_status,
        nppm_user,
        mpo_user,
        audit_flag,
        template,
        tan,
        cord_entry,
        commit_audit
    FROM {{ source('raw', 'co_project') }}
),

source_co_snprefix AS (
    SELECT
        snprefix_id,
        project_id,
        snprefix,
        coo,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date
    FROM {{ source('raw', 'co_snprefix') }}
),

transformed_exptrans AS (
    SELECT
    snprefix_id,
    project_id,
    snprefix,
    coo,
    created_by,
    created_date,
    last_updated_by,
    last_updated_date,
    'BATCHID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_co_snprefix
),

final AS (
    SELECT
        batch_id,
        snprefix_id,
        project_id,
        snprefix,
        coo,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        create_datetime,
        action_code
    FROM transformed_exptrans
)

SELECT * FROM final
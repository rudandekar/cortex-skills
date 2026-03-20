{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_xxscm_pl_oe_audit_attr_bi', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXSCM_PL_OE_AUDIT_ATTR_BI',
        'target_table': 'CG1_XXSCM_PL_OE_AUDIT_ATTR_BI',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.610323+00:00'
    }
) }}

WITH 

source_cg1_xxscm_pl_oe_audit_attr_bi AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        sequence_id,
        entity_id,
        entity_display_name,
        attribute_id,
        attribute_display_name,
        old_display_value,
        new_display_value,
        hist_creation_date,
        order_number,
        entity_number,
        org_id,
        reason,
        hist_comments,
        user_id,
        user_name,
        responsibility_id,
        responsibility_name,
        old_context_value,
        new_context_value,
        created_by,
        creation_date,
        last_updated_date,
        last_updated_by,
        request_id
    FROM {{ source('raw', 'cg1_xxscm_pl_oe_audit_attr_bi') }}
),

source_stg_cg1_xxscm_pl_oe_audit_attr AS (
    SELECT
        sequence_id,
        entity_id,
        entity_display_name,
        attribute_id,
        attribute_display_name,
        old_display_value,
        new_display_value,
        hist_creation_date,
        order_number,
        entity_number,
        org_id,
        reason,
        hist_comments,
        user_id,
        user_name,
        responsibility_id,
        responsibility_name,
        old_context_value,
        new_context_value,
        created_by,
        creation_date,
        last_updated_date,
        last_updated_by,
        request_id,
        source_dml_type,
        trail_file_name,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_cg1_xxscm_pl_oe_audit_attr') }}
),

transformed_exp_cg1_xxscm_pl_oe_audit_attr_bi AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    sequence_id,
    entity_id,
    entity_display_name,
    attribute_id,
    attribute_display_name,
    old_display_value,
    new_display_value,
    hist_creation_date,
    order_number,
    entity_number,
    org_id,
    reason,
    hist_comments,
    user_id,
    user_name,
    responsibility_id,
    responsibility_name,
    old_context_value,
    new_context_value,
    created_by,
    creation_date,
    last_updated_date,
    last_updated_by,
    request_id
    FROM source_stg_cg1_xxscm_pl_oe_audit_attr
),

final AS (
    SELECT
        sequence_id,
        entity_id,
        entity_display_name,
        attribute_id,
        attribute_display_name,
        old_display_value,
        new_display_value,
        hist_creation_date,
        order_number,
        entity_number,
        org_id,
        reason,
        hist_comments,
        user_id,
        user_name,
        responsibility_id,
        responsibility_name,
        old_context_value,
        new_context_value,
        created_by,
        creation_date,
        last_updated_date,
        last_updated_by,
        request_id,
        source_dml_type,
        trail_file_name,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_cg1_xxscm_pl_oe_audit_attr_bi
)

SELECT * FROM final
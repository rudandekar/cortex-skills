{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_xxgco_oe_order_contacts', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXGCO_OE_ORDER_CONTACTS',
        'target_table': 'CG1_XXGCO_OE_ORD_CONT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.585361+00:00'
    }
) }}

WITH 

source_cg1_xxgco_oe_order_contacts AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        header_id,
        last_name,
        first_name,
        email_address,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        contact_type,
        telephone
    FROM {{ source('raw', 'cg1_xxgco_oe_order_contacts') }}
),

source_stg_cg1_xxgco_oe_order_contact AS (
    SELECT
        header_id,
        last_name,
        first_name,
        email_address,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        contact_type,
        telephone,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_cg1_xxgco_oe_order_contact') }}
),

transformed_exp_cg1_xxgco_oe_order_contacts AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    header_id,
    last_name,
    first_name,
    email_address,
    creation_date,
    created_by,
    last_updated_by,
    last_update_date,
    contact_type,
    telephone
    FROM source_stg_cg1_xxgco_oe_order_contact
),

final AS (
    SELECT
        header_id,
        last_name,
        first_name,
        email_address,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        contact_type,
        telephone,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_cg1_xxgco_oe_order_contacts
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_contact_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_CONTACT_RNWL',
        'target_table': 'W_DEAL_CONTACT_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.990232+00:00'
    }
) }}

WITH 

source_st_cq_deal_contact_rnwl AS (
    SELECT
        batch_id,
        deal_object_id,
        object_id,
        contact_id,
        contact_name,
        contact_type,
        created_on,
        created_by,
        updated_on,
        updated_by,
        contact_row,
        dm_update_date,
        contact_postn_id,
        siebel_postn_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cq_deal_contact_rnwl') }}
),

source_ex_cq_deal_contact_rnwl AS (
    SELECT
        batch_id,
        deal_object_id,
        object_id,
        contact_id,
        contact_name,
        contact_type,
        created_on,
        created_by,
        updated_on,
        updated_by,
        contact_row,
        dm_update_date,
        contact_postn_id,
        siebel_postn_id,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_cq_deal_contact_rnwl') }}
),

final AS (
    SELECT
        bk_deal_id,
        bk_src_reported_contact_name,
        src_rptd_contact_type_cd,
        src_rptd_contact_subtype_cd,
        contact_cisco_worker_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_ex_cq_deal_contact_rnwl
)

SELECT * FROM final
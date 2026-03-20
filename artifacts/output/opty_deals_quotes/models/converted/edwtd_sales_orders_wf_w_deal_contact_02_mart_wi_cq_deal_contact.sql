{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_contact', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_CONTACT',
        'target_table': 'WI_CQ_DEAL_CONTACT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.905709+00:00'
    }
) }}

WITH 

source_ex_cq_deal_contact AS (
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
    FROM {{ source('raw', 'ex_cq_deal_contact') }}
),

source_st_cq_deal_contact AS (
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
    FROM {{ source('raw', 'st_cq_deal_contact') }}
),

final AS (
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
    FROM source_st_cq_deal_contact
)

SELECT * FROM final
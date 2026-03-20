{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ccrm_profile_order', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_CCRM_PROFILE_ORDER',
        'target_table': 'ST_CCRM_PROFILE_ORDER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.182630+00:00'
    }
) }}

WITH 

source_ff_ccrm_profile_order AS (
    SELECT
        batch_id,
        profile_id,
        header_id,
        order_number,
        global_name,
        global_deal_id,
        aph_flag,
        book_def_flag,
        rev_def_flag,
        cogs_def_flag,
        ord_cycle_open_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        new_order_flag,
        workflow_flag,
        processed,
        archived_flag,
        order_status,
        current_datetime,
        action_code
    FROM {{ source('raw', 'ff_ccrm_profile_order') }}
),

final AS (
    SELECT
        batch_id,
        profile_id,
        header_id,
        order_number,
        global_name,
        global_deal_id,
        aph_flag,
        book_def_flag,
        rev_def_flag,
        cogs_def_flag,
        ord_cycle_open_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        new_order_flag,
        workflow_flag,
        processed,
        archived_flag,
        order_status,
        creation_datetime,
        action_code
    FROM source_ff_ccrm_profile_order
)

SELECT * FROM final
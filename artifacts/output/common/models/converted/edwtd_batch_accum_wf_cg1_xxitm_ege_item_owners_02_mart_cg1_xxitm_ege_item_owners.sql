{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_xxitm_ege_item_owners', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXITM_EGE_ITEM_OWNERS',
        'target_table': 'CG1_XXITM_EGE_ITEM_OWNERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.759161+00:00'
    }
) }}

WITH 

source_cg1_xxitm_ege_item_owners AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        itm_own_id,
        inventory_item_id,
        organization_id,
        item_name,
        owner,
        owner_id,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        last_transaction_date
    FROM {{ source('raw', 'cg1_xxitm_ege_item_owners') }}
),

source_stg_cg1_xxitm_ege_item_owners AS (
    SELECT
        itm_own_id,
        inventory_item_id,
        organization_id,
        item_name,
        owner,
        owner_id,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        last_transaction_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_xxitm_ege_item_owners') }}
),

transformed_exp_cg1_xxitm_ege_item_owners AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    itm_own_id,
    inventory_item_id,
    organization_id,
    item_name,
    owner,
    owner_id,
    creation_date,
    created_by,
    last_updated_by,
    last_update_date,
    last_transaction_date
    FROM source_stg_cg1_xxitm_ege_item_owners
),

final AS (
    SELECT
        itm_own_id,
        inventory_item_id,
        organization_id,
        item_name,
        owner,
        owner_id,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        last_transaction_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_xxitm_ege_item_owners
)

SELECT * FROM final
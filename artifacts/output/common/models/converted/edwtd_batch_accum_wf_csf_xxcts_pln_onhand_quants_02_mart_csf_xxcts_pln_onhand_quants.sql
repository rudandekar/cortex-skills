{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_xxcts_pln_onhand_quants', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCTS_PLN_ONHAND_QUANTS',
        'target_table': 'CSF_XXCTS_PLN_ONHAND_QUANTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.014791+00:00'
    }
) }}

WITH 

source_stg_xxcts_pln_onhand_quants AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        inventory_item_id,
        organization_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        subinventory_code,
        onhand_quantity,
        intransit_quantity
    FROM {{ source('raw', 'stg_xxcts_pln_onhand_quants') }}
),

source_csf_xxcts_pln_onhand_quants AS (
    SELECT
        inventory_item_id,
        organization_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        subinventory_code,
        onhand_quantity,
        intransit_quantity,
        ges_update_date
    FROM {{ source('raw', 'csf_xxcts_pln_onhand_quants') }}
),

transformed_exptrans AS (
    SELECT
    ges_update_date,
    inventory_item_id,
    organization_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    subinventory_code,
    onhand_quantity,
    intransit_quantity,
    source_commit_time,
    'INSERT' AS source_dml_type
    FROM source_csf_xxcts_pln_onhand_quants
),

final AS (
    SELECT
        source_dml_type,
        ges_update_date,
        refresh_datetime,
        inventory_item_id,
        organization_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        subinventory_code,
        onhand_quantity,
        intransit_quantity
    FROM transformed_exptrans
)

SELECT * FROM final
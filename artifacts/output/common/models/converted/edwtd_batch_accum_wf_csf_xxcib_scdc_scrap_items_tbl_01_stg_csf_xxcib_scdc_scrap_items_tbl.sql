{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_xxcib_scdc_scrap_items_tbl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCIB_SCDC_SCRAP_ITEMS_TBL',
        'target_table': 'STG_CSF_XXCIB_SCDC_SCRAP_ITEMS_TBL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.917219+00:00'
    }
) }}

WITH 

source_stg_csf_xxcib_scdc_scrap_items_tbl AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        message_id,
        file_id,
        esr_number,
        partner_code,
        theatre,
        status_code,
        effective_date,
        part_id_type,
        part_id,
        serial_number,
        supplier_ref,
        rma_number,
        created_date,
        created_by,
        update_date,
        update_by,
        active_flag,
        product_family,
        inventory_item_id
    FROM {{ source('raw', 'stg_csf_xxcib_scdc_scrap_items_tbl') }}
),

source_csf_xxcib_scdc_scrap_items_tbl AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        message_id,
        file_id,
        esr_number,
        partner_code,
        theatre,
        status_code,
        effective_date,
        part_id_type,
        part_id,
        serial_number,
        supplier_ref,
        rma_number,
        created_date,
        created_by,
        update_date,
        update_by,
        active_flag,
        product_family,
        inventory_item_id
    FROM {{ source('raw', 'csf_xxcib_scdc_scrap_items_tbl') }}
),

transformed_exptrans AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    message_id,
    file_id,
    esr_number,
    partner_code,
    theatre,
    status_code,
    effective_date,
    part_id_type,
    part_id,
    serial_number,
    supplier_ref,
    rma_number,
    created_date,
    created_by,
    update_date,
    update_by,
    active_flag,
    product_family,
    inventory_item_id
    FROM source_csf_xxcib_scdc_scrap_items_tbl
),

final AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        message_id,
        file_id,
        esr_number,
        partner_code,
        theatre,
        status_code,
        effective_date,
        part_id_type,
        part_id,
        serial_number,
        supplier_ref,
        rma_number,
        created_date,
        created_by,
        update_date,
        update_by,
        active_flag,
        product_family,
        inventory_item_id
    FROM transformed_exptrans
)

SELECT * FROM final
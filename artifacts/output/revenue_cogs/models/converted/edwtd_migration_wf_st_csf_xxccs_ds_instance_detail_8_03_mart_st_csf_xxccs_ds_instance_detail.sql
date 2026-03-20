{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_csf_xxccs_ds_instance_detail', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_FF_CSF_XXCCS_DS_INSTANCE_DETAIL',
        'target_table': 'ST_CSF_XXCCS_DS_INSTANCE_DETAIL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.350086+00:00'
    }
) }}

WITH 

source_ff_csf_xxccs_ds_instance_detail AS (
    SELECT
        instance_id,
        serial_number,
        ib_status,
        warranty_end_date,
        creation_date,
        last_update_date,
        erp_ship_date,
        edw_create_dtm,
        warranty_type
    FROM {{ source('raw', 'ff_csf_xxccs_ds_instance_detail') }}
),

source_csf_xxccs_ds_instance_detail AS (
    SELECT
        instance_id,
        serial_number,
        ib_status,
        warranty_end_date,
        creation_date,
        last_update_date,
        erp_ship_date,
        warranty_type
    FROM {{ source('raw', 'csf_xxccs_ds_instance_detail') }}
),

final AS (
    SELECT
        instance_id,
        serial_number,
        ib_status,
        warranty_end_date,
        creation_date,
        last_update_date,
        erp_ship_date,
        edw_create_dtm,
        warranty_type
    FROM source_csf_xxccs_ds_instance_detail
)

SELECT * FROM final
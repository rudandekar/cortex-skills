{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcmf_sp_bts_ext_pidtan', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_FF_XXCMF_SP_BTS_EXT_PIDTAN',
        'target_table': 'FF_XXCMF_SP_BTS_EXT_PIDTAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.081191+00:00'
    }
) }}

WITH 

source_cg1_xxcmf_sp_bts_extract_pid_tan AS (
    SELECT
        tan,
        tan_desc,
        tan_organization_code,
        pid,
        pid_desc,
        pid_organization_code,
        bucket_date,
        sales_order,
        unconsumed_forecast,
        released_po,
        planned_po,
        publish_date,
        creation_date,
        created_by,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'cg1_xxcmf_sp_bts_extract_pid_tan') }}
),

transformed_exp_xxcmf_sp_bts_extract_pid_tan AS (
    SELECT
    tan,
    tan_desc,
    tan_organization_code,
    pid,
    pid_desc,
    pid_organization_code,
    bucket_date,
    sales_order,
    unconsumed_forecast,
    released_po,
    planned_po,
    publish_date,
    creation_date,
    created_by,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_cg1_xxcmf_sp_bts_extract_pid_tan
),

final AS (
    SELECT
        tan,
        tan_desc,
        tan_organization_code,
        pid,
        pid_desc,
        pid_organization_code,
        bucket_date,
        sales_order,
        unconsumed_forecast,
        released_po,
        planned_po,
        publish_date,
        creation_date,
        created_by,
        batch_id,
        create_datetime,
        action_code
    FROM transformed_exp_xxcmf_sp_bts_extract_pid_tan
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcmf_sp_bts_ext_pidtan', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_XXCMF_SP_BTS_EXT_PIDTAN',
        'target_table': 'ST_XXCMF_SP_BTS_EXT_PIDTAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.013102+00:00'
    }
) }}

WITH 

source_ff_xxcmf_sp_bts_ext_pidtan AS (
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
    FROM {{ source('raw', 'ff_xxcmf_sp_bts_ext_pidtan') }}
),

final AS (
    SELECT
        tan_id,
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
    FROM source_ff_xxcmf_sp_bts_ext_pidtan
)

SELECT * FROM final
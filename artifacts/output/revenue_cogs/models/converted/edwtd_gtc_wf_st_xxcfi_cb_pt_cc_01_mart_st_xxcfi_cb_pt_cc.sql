{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcfi_cb_pt_cc', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_XXCFI_CB_PT_CC',
        'target_table': 'ST_XXCFI_CB_PT_CC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.650128+00:00'
    }
) }}

WITH 

source_ff_xxcfi_cb_pt_cc AS (
    SELECT
        batch_id,
        pt_cc_id,
        name,
        description,
        product_flag,
        start_date,
        end_date,
        enable_flag,
        created_by,
        created_date,
        modified_by,
        modified_date,
        source1,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_xxcfi_cb_pt_cc') }}
),

final AS (
    SELECT
        batch_id,
        pt_cc_id,
        name,
        description,
        product_flag,
        start_date,
        end_date,
        enable_flag,
        created_by,
        created_date,
        modified_by,
        modified_date,
        source1,
        create_datetime,
        action_code
    FROM source_ff_xxcfi_cb_pt_cc
)

SELECT * FROM final
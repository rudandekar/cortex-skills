{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_wips_inv_trans_details', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_WIPS_INV_TRANS_DETAILS',
        'target_table': 'ST_WIPS_INV_TRANS_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.452031+00:00'
    }
) }}

WITH 

source_ff_wips_inv_trans_details AS (
    SELECT
        inv_trans_id,
        calc_in_transit_qty,
        src_batch_id,
        detail_id,
        detail_type,
        detail_value,
        active_flag,
        created_date,
        last_update_date,
        create_datetime,
        batch_id,
        action_code
    FROM {{ source('raw', 'ff_wips_inv_trans_details') }}
),

final AS (
    SELECT
        inv_trans_id,
        calc_in_transit_qty,
        src_batch_id,
        detail_id,
        detail_type,
        detail_value,
        active_flag,
        created_date,
        last_update_date,
        create_datetime,
        batch_id,
        action_code
    FROM source_ff_wips_inv_trans_details
)

SELECT * FROM final
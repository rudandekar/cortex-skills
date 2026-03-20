{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dca_details', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_DCA_DETAILS',
        'target_table': 'ST_DCA_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.571814+00:00'
    }
) }}

WITH 

source_ff_dca_details AS (
    SELECT
        dca_id,
        dca_detail_id,
        claim_id,
        active_flag,
        last_update_date,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_dca_details') }}
),

final AS (
    SELECT
        dca_id,
        dca_detail_id,
        claim_id,
        active_flag,
        last_update_date,
        batch_id,
        create_datetime,
        action_code
    FROM source_ff_dca_details
)

SELECT * FROM final
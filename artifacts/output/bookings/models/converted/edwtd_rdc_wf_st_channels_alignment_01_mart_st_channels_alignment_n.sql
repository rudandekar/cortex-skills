{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_channels_alignment', 'batch', 'edwtd_rdc'],
    meta={
        'source_workflow': 'wf_m_ST_CHANNELS_ALIGNMENT',
        'target_table': 'ST_CHANNELS_ALIGNMENT_N',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.376554+00:00'
    }
) }}

WITH 

source_ff_channels_alignment_ftp AS (
    SELECT
        sub_promo_name,
        channel_alignment,
        status,
        promotion_name,
        promotion_type,
        batch_id,
        action_code,
        create_datetime
    FROM {{ source('raw', 'ff_channels_alignment_ftp') }}
),

transformed_exp_channel_alignment AS (
    SELECT
    channel_alignment,
    promotion_name,
    status,
    sub_promo_name,
    promotion_type,
    'BatchId' AS batch_id,
    'I' AS action_code,
    CURRENT_TIMESTAMP() AS create_datetime
    FROM source_ff_channels_alignment_ftp
),

final AS (
    SELECT
        sub_promo_name,
        channel_alignment,
        status,
        promotion_name,
        promotion_type,
        batch_id,
        action_code,
        create_datetime
    FROM transformed_exp_channel_alignment
)

SELECT * FROM final
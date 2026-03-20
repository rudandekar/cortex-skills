{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_w_pricing_incentive', 'batch', 'edwtd_rdc'],
    meta={
        'source_workflow': 'wf_m_W_PRICING_INCENTIVE',
        'target_table': 'W_PRICING_INCENTIVE_TEMP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.675144+00:00'
    }
) }}

WITH 

source_st_channels_alignment AS (
    SELECT
        sub_promo_name,
        channel_alignment,
        status,
        promotion_name,
        promotion_type,
        batch_id,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_channels_alignment') }}
),

final AS (
    SELECT
        bk_pricing_incentive_name,
        channel_prmtn_algnmt_cd,
        aligned_status_flg,
        pricing_incentive_group_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        pricing_incentive_type_cd,
        action_code,
        dml_type
    FROM source_st_channels_alignment
)

SELECT * FROM final
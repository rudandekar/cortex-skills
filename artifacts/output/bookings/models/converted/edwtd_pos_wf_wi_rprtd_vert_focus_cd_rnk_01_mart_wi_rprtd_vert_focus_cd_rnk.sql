{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rprtd_vert_focus_cd_rnk', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_WI_RPRTD_VERT_FOCUS_CD_RNK',
        'target_table': 'WI_RPRTD_VERT_FOCUS_CD_RNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.902043+00:00'
    }
) }}

WITH 

source_wi_rprtd_vert_focus_cd_rnk AS (
    SELECT
        bk_pos_transaction_id_int,
        bk_src_rprtd_vert_focus_cd,
        rnk
    FROM {{ source('raw', 'wi_rprtd_vert_focus_cd_rnk') }}
),

final AS (
    SELECT
        bk_pos_transaction_id_int,
        bk_src_rprtd_vert_focus_cd,
        rnk
    FROM source_wi_rprtd_vert_focus_cd_rnk
)

SELECT * FROM final
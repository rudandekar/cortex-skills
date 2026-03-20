{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_pos_rsllr_vert_indr_focs', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_MT_POS_RSLLR_VERT_INDR_FOCS',
        'target_table': 'MT_POS_RSLLR_VERT_INDR_FOCS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.253785+00:00'
    }
) }}

WITH 

source_mt_pos_rsllr_vert_indr_focs AS (
    SELECT
        bk_pos_transaction_id_int,
        bk_src_rprtd_vert_focus_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_pos_rsllr_vert_indr_focs') }}
),

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
        dv_src_rprtd_vert_focus_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_wi_rprtd_vert_focus_cd_rnk
)

SELECT * FROM final
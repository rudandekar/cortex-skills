{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_disti_prmtn_apprvd_disti', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DISTI_PRMTN_APPRVD_DISTI',
        'target_table': 'N_DISTI_PRMTN_APPRVD_DISTI',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.326004+00:00'
    }
) }}

WITH 

source_w_disti_prmtn_apprvd_disti AS (
    SELECT
        bk_wips_originator_id_int,
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_disti_prmtn_apprvd_disti') }}
),

final AS (
    SELECT
        bk_wips_originator_id_int,
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_disti_prmtn_apprvd_disti
)

SELECT * FROM final
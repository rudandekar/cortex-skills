{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_adjustment_subreason', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_ADJUSTMENT_SUBREASON',
        'target_table': 'N_CCRM_ADJUSTMENT_SUBREASON',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.749942+00:00'
    }
) }}

WITH 

source_w_ccrm_adjustment_subreason AS (
    SELECT
        bk_ccrm_adj_subreason_cd,
        bk_ccrm_adj_subrsn_ver_num_int,
        subreason_descr,
        sk_subreason_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_adjustment_subreason') }}
),

final AS (
    SELECT
        bk_ccrm_adj_subreason_cd,
        bk_ccrm_adj_subrsn_ver_num_int,
        subreason_descr,
        sk_subreason_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ccrm_adjustment_subreason
)

SELECT * FROM final
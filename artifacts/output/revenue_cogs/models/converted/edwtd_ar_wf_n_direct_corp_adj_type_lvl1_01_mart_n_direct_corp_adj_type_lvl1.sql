{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_direct_corp_adj_type_lvl1', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_DIRECT_CORP_ADJ_TYPE_LVL1',
        'target_table': 'N_DIRECT_CORP_ADJ_TYPE_LVL1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.312420+00:00'
    }
) }}

WITH 

source_w_direct_corp_adj_type_lvl1 AS (
    SELECT
        bk_direct_corp_adj_lvl1_typ_cd,
        direct_corp_adj_lvl1_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'w_direct_corp_adj_type_lvl1') }}
),

final AS (
    SELECT
        bk_direct_corp_adj_lvl1_typ_cd,
        direct_corp_adj_lvl1_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_direct_corp_adj_type_lvl1
)

SELECT * FROM final
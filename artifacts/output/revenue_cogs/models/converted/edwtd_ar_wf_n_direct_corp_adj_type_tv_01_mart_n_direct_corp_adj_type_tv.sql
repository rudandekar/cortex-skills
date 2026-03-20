{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_direct_corp_adj_type_tv', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_DIRECT_CORP_ADJ_TYPE_TV',
        'target_table': 'N_DIRECT_CORP_ADJ_TYPE_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.267838+00:00'
    }
) }}

WITH 

source_w_direct_corp_adj_type AS (
    SELECT
        bk_direct_corp_adj_type_cd,
        bk_direct_corp_adj_lvl2_typ_cd,
        start_tv_dt,
        end_tv_dt,
        direct_corp_adj_end_dtm,
        direct_corp_adj_start_dtm,
        direct_corp_adj_descr,
        sk_adj_type_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_direct_corp_adj_type') }}
),

final AS (
    SELECT
        bk_direct_corp_adj_type_cd,
        bk_direct_corp_adj_lvl2_typ_cd,
        start_tv_dt,
        end_tv_dt,
        direct_corp_adj_end_dtm,
        direct_corp_adj_start_dtm,
        direct_corp_adj_descr,
        sk_adj_type_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_direct_corp_adj_type
)

SELECT * FROM final
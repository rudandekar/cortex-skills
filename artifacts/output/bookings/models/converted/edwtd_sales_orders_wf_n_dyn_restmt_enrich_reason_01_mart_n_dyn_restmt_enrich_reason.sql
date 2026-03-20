{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_dyn_restmt_enrich_reason', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DYN_RESTMT_ENRICH_REASON',
        'target_table': 'N_DYN_RESTMT_ENRICH_REASON',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.918076+00:00'
    }
) }}

WITH 

source_st_xxanp_fin_sav_descr AS (
    SELECT
        reason_cd,
        new_rstmnt_type_name
    FROM {{ source('raw', 'st_xxanp_fin_sav_descr') }}
),

final AS (
    SELECT
        bk_reason_cd,
        reason_descr,
        restatement_type_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_xxanp_fin_sav_descr
)

SELECT * FROM final
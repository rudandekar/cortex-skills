{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sls_terr_rstmnt_mapping', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_N_SLS_TERR_RSTMNT_MAPPING',
        'target_table': 'N_SLS_TERR_RSTMNT_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.383387+00:00'
    }
) }}

WITH 

source_n_sls_terr_rstmnt_mapping AS (
    SELECT
        orig_sls_terr_key,
        rstd_sls_terr_key,
        src_rprtd_orig_sls_terr_name,
        rstd_node_defaulted_level_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        restatement_dt,
        source_deleted_flg
    FROM {{ source('raw', 'n_sls_terr_rstmnt_mapping') }}
),

final AS (
    SELECT
        orig_sls_terr_key,
        rstd_sls_terr_key,
        src_rprtd_orig_sls_terr_name,
        rstd_node_defaulted_level_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        restatement_dt,
        source_deleted_flg
    FROM source_n_sls_terr_rstmnt_mapping
)

SELECT * FROM final
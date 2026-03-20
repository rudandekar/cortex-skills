{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pss_sub_technology', 'batch', 'edwtd_mkt_cic'],
    meta={
        'source_workflow': 'wf_m_N_PSS_SUB_TECHNOLOGY',
        'target_table': 'N_PSS_SUB_TECHNOLOGY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.430792+00:00'
    }
) }}

WITH 

source_w_pss_sub_technology AS (
    SELECT
        bk_pss_technology_name,
        bk_pss_sub_technology_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_pss_sub_technology') }}
),

final AS (
    SELECT
        bk_pss_technology_name,
        bk_pss_sub_technology_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_pss_sub_technology
)

SELECT * FROM final
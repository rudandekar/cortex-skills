{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cogs_overhead_type', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_COGS_OVERHEAD_TYPE',
        'target_table': 'N_COGS_OVERHEAD_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.246133+00:00'
    }
) }}

WITH 

source_n_cogs_overhead_type AS (
    SELECT
        bk_fiscal_cal_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_mth_num_int,
        bk_overhead_type_name,
        overhead_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_cogs_overhead_type') }}
),

final AS (
    SELECT
        bk_fiscal_cal_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_mth_num_int,
        bk_overhead_type_name,
        overhead_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_cogs_overhead_type
)

SELECT * FROM final
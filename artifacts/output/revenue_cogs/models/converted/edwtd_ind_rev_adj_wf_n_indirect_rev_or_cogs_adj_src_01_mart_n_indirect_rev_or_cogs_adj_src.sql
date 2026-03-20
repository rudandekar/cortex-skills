{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_indirect_rev_or_cogs_adj_src', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_N_INDIRECT_REV_OR_COGS_ADJ_SRC',
        'target_table': 'N_INDIRECT_REV_OR_COGS_ADJ_SRC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.512830+00:00'
    }
) }}

WITH 

source_w_indirect_rev_or_cogs_adj_src AS (
    SELECT
        bk_adjustment_source_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_indirect_rev_or_cogs_adj_src') }}
),

final AS (
    SELECT
        bk_adjustment_source_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_indirect_rev_or_cogs_adj_src
)

SELECT * FROM final
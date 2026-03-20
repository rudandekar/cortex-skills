{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_indirect_rev_or_cogs_adj_src', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_WK_INDIRECT_REV_OR_COGS_ADJ_SRC',
        'target_table': 'W_INDIRECT_REV_OR_COGS_ADJ_SRC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.638549+00:00'
    }
) }}

WITH 

source_st_ae_data_sources AS (
    SELECT
        batch_id,
        source_system_id,
        source_system_name,
        source_system_type_code,
        source_system_category_label,
        status,
        create_user,
        update_user,
        update_datetime,
        create_datetime,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'st_ae_data_sources') }}
),

final AS (
    SELECT
        bk_adjustment_source_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM source_st_ae_data_sources
)

SELECT * FROM final
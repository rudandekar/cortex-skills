{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_restate3_edw_hier_mapping', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_ST_RESTATE3_EDW_HIER_MAPPING',
        'target_table': 'ST_RESTATE3_EDW_HIER_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.918286+00:00'
    }
) }}

WITH 

source_ff_restate3_edw_hier_mapping AS (
    SELECT
        actual_share_node_id,
        act_sales_territory_name_code,
        actual_sales_territory_key,
        restated_share_node_id,
        default_level,
        restatement_dt,
        source_deleted_flg
    FROM {{ source('raw', 'ff_restate3_edw_hier_mapping') }}
),

final AS (
    SELECT
        actual_share_node_id,
        act_sales_territory_name_code,
        actual_sales_territory_key,
        restated_share_node_id,
        default_level,
        restatement_dt,
        source_deleted_flg
    FROM source_ff_restate3_edw_hier_mapping
)

SELECT * FROM final
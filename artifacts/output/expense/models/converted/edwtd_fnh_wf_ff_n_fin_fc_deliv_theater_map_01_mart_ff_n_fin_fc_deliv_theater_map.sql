{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_n_fin_fc_deliv_theater_map', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_FF_N_FIN_FC_DELIV_THEATER_MAP',
        'target_table': 'FF_N_FIN_FC_DELIV_THEATER_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.910612+00:00'
    }
) }}

WITH 

source_vw_n_fin_fc_deliv_theater_map AS (
    SELECT
        parent_node,
        child_node,
        child_node_desc,
        refresh_date
    FROM {{ source('raw', 'vw_n_fin_fc_deliv_theater_map') }}
),

transformed_ex_ff_n_fin_fc_deliv_theater_map AS (
    SELECT
    parent_node,
    child_node,
    child_node_desc,
    refresh_date,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code,
    'BatchId' AS batch_id
    FROM source_vw_n_fin_fc_deliv_theater_map
),

final AS (
    SELECT
        parent_node,
        child_node,
        child_node_desc,
        refresh_date,
        create_datetime,
        action_code,
        batch_id
    FROM transformed_ex_ff_n_fin_fc_deliv_theater_map
)

SELECT * FROM final
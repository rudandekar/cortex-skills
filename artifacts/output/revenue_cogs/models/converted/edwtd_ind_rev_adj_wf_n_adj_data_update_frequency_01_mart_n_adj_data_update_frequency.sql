{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_adj_data_update_frequency', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_N_ADJ_DATA_UPDATE_FREQUENCY',
        'target_table': 'N_ADJ_DATA_UPDATE_FREQUENCY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.694575+00:00'
    }
) }}

WITH 

source_w_adj_data_update_frequency AS (
    SELECT
        bk_adj_data_update_freq_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_adj_data_update_frequency') }}
),

final AS (
    SELECT
        bk_adj_data_update_freq_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_adj_data_update_frequency
)

SELECT * FROM final
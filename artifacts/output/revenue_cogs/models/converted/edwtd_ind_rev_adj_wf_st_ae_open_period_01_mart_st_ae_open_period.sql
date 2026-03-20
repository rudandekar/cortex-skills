{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_open_period', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_ST_AE_OPEN_PERIOD',
        'target_table': 'ST_AE_OPEN_PERIOD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.853301+00:00'
    }
) }}

WITH 

source_st_ae_open_period AS (
    SELECT
        batch_id,
        fiscal_month_id,
        open_flag,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'st_ae_open_period') }}
),

final AS (
    SELECT
        batch_id,
        fiscal_month_id,
        open_flag,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        create_timestamp,
        action_code
    FROM source_st_ae_open_period
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_open_period_all', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_ST_AE_OPEN_PERIOD_ALL',
        'target_table': 'ST_AE_OPEN_PERIOD_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.273921+00:00'
    }
) }}

WITH 

source_st_ae_open_period_all AS (
    SELECT
        module_id,
        module_name,
        fiscal_month_id,
        open_flag,
        create_user,
        create_datetime,
        update_user,
        update_datetime
    FROM {{ source('raw', 'st_ae_open_period_all') }}
),

final AS (
    SELECT
        module_id,
        module_name,
        fiscal_month_id,
        open_flag,
        create_user,
        create_datetime,
        update_user,
        update_datetime
    FROM source_st_ae_open_period_all
)

SELECT * FROM final
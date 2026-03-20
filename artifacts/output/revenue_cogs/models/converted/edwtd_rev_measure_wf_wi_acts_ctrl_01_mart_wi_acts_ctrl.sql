{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_acts_ctrl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_ACTS_CTRL',
        'target_table': 'WI_ACTS_CTRL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.633205+00:00'
    }
) }}

WITH 

source_wi_acts_ctrl AS (
    SELECT
        fiscal_month_name,
        fiscal_month_id,
        fiscal_month_status_ind,
        fiscal_year_id,
        fiscal_month_start_date,
        fiscal_month_start_date_adj,
        fiscal_month_end_date,
        fiscal_month_end_date_adj,
        prev_month_start_date_adj,
        reload_flag,
        workgroup_backfill_flag
    FROM {{ source('raw', 'wi_acts_ctrl') }}
),

final AS (
    SELECT
        fiscal_month_name,
        fiscal_month_id,
        fiscal_month_status_ind,
        fiscal_year_id,
        fiscal_month_start_date,
        fiscal_month_start_date_adj,
        fiscal_month_end_date,
        fiscal_month_end_date_adj,
        prev_month_start_date_adj,
        reload_flag,
        workgroup_backfill_flag
    FROM source_wi_acts_ctrl
)

SELECT * FROM final
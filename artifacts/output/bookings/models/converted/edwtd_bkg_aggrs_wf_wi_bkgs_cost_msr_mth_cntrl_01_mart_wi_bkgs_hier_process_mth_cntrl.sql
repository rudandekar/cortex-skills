{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_bkgs_cost_msr_mth_cntrl', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_BKGS_COST_MSR_MTH_CNTRL',
        'target_table': 'WI_BKGS_HIER_PROCESS_MTH_CNTRL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.747634+00:00'
    }
) }}

WITH 

source_wi_bkgs_hier_anty_mth_cntrl AS (
    SELECT
        fiscal_year_mth_number_int,
        run_batch_id,
        inc_flag
    FROM {{ source('raw', 'wi_bkgs_hier_anty_mth_cntrl') }}
),

source_wi_bkgs_hier_process_mth_cntrl AS (
    SELECT
        fiscal_year_week_num_int,
        run_batch_id,
        inc_flag
    FROM {{ source('raw', 'wi_bkgs_hier_process_mth_cntrl') }}
),

final AS (
    SELECT
        fiscal_year_week_num_int,
        run_batch_id,
        inc_flag
    FROM source_wi_bkgs_hier_process_mth_cntrl
)

SELECT * FROM final
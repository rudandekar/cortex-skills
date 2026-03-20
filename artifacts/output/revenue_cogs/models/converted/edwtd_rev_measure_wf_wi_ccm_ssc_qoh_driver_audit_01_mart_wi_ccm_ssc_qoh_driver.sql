{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ccm_ssc_qoh_driver', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CCM_SSC_QOH_DRIVER',
        'target_table': 'WI_CCM_SSC_QOH_DRIVER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.022394+00:00'
    }
) }}

WITH 

source_gps_inv_on_hand_ccm AS (
    SELECT
        fiscal_month_id,
        theater,
        inv_cost_amt
    FROM {{ source('raw', 'gps_inv_on_hand_ccm') }}
),

lookup_lkp_wi_tss_cost_cntrl_month AS (
    SELECT
        a.*,
        b.*
    FROM source_gps_inv_on_hand_ccm a
    LEFT JOIN {{ source('raw', '$$stgdb.wi_tss_cost_cntrl_month_id') }} b
        ON a.in_fiscal_month_id = b.in_fiscal_month_id
),

transformed_exp_ccm_ssc_qoh_driver AS (
    SELECT
    fiscal_month_id,
    theater,
    inv_cost_amt
    FROM lookup_lkp_wi_tss_cost_cntrl_month
),

filtered_fil_ccm_ssc_qoh_driver AS (
    SELECT *
    FROM transformed_exp_ccm_ssc_qoh_driver
    WHERE NOT ISNULL(FISCAL_MONTH_ID)
),

final AS (
    SELECT
        fiscal_month_id,
        theater,
        inv_cost_amt,
        inv_percentage
    FROM filtered_fil_ccm_ssc_qoh_driver
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcmf_sp_disti_inv_aging_dtl', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_XXCMF_SP_DISTI_INV_AGING_DTL',
        'target_table': 'ST_XXCMF_SP_DISTI_INV_AGI_DTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.677583+00:00'
    }
) }}

WITH 

source_ff_xxcmf_sp_disti_inv_aging_dtl AS (
    SELECT
        disti_parent_name,
        product_family,
        publish_date,
        inv_aging_0_30_days,
        inv_aging_31_60_days,
        inv_aging_61_90_days,
        inv_aging_91_120_days,
        inv_aging_121_180_days,
        inv_aging_181_365_days,
        inv_aging_gt_1yr,
        creation_date,
        created_by
    FROM {{ source('raw', 'ff_xxcmf_sp_disti_inv_aging_dtl') }}
),

final AS (
    SELECT
        distributor_master_name,
        product_family_id,
        publication_dt,
        dr_pf_inv_vl_0_30_day_usd_amt,
        dr_pf_inv_vl_31_60_day_usd_amt,
        dr_pf_inv_vl_61_90_day_usd_amt,
        dr_pf_inv_vl_91_120day_usd_amt,
        dr_pf_inv_vl121_180day_usd_amt,
        dr_pf_inv_vl181_365day_usd_amt,
        dr_pf_inv_vl_ovr_1yr_usd_amt,
        creation_date,
        created_by
    FROM source_ff_xxcmf_sp_disti_inv_aging_dtl
)

SELECT * FROM final
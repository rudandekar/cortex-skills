{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sply_plng_dst_pf_inv_age_lnk', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_SPLY_PLNG_DST_PF_INV_AGE_LNK',
        'target_table': 'W_SPLY_PLNG_DST_PF_INV_AGE_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.536076+00:00'
    }
) }}

WITH 

source_st_xxcmf_sp_disti_inv_agi_dtl AS (
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
    FROM {{ source('raw', 'st_xxcmf_sp_disti_inv_agi_dtl') }}
),

transformed_exptrans AS (
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
    created_by,
    'I' AS action_code,
    'I' AS dml_type
    FROM source_st_xxcmf_sp_disti_inv_agi_dtl
),

final AS (
    SELECT
        bk_distributor_master_name,
        bk_product_family_id,
        bk_publication_dt,
        dr_pf_inv_vl_0_30_day_usd_amt,
        dr_pf_inv_vl_31_60_day_usd_amt,
        dr_pf_inv_vl_61_90_day_usd_amt,
        dr_pf_inv_vl_91_120day_usd_amt,
        dr_pf_inv_vl121_180day_usd_amt,
        dr_pf_inv_vl181_365day_usd_amt,
        dr_pf_inv_vl_ovr_1yr_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exptrans
)

SELECT * FROM final
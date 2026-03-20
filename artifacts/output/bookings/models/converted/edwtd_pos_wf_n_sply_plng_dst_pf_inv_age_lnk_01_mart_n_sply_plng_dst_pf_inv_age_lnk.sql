{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sply_plng_dst_pf_inv_age_lnk', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_SPLY_PLNG_DST_PF_INV_AGE_LNK',
        'target_table': 'N_SPLY_PLNG_DST_PF_INV_AGE_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.383089+00:00'
    }
) }}

WITH 

source_w_sply_plng_dst_pf_inv_age_lnk AS (
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
    FROM {{ source('raw', 'w_sply_plng_dst_pf_inv_age_lnk') }}
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
        edw_update_user
    FROM source_w_sply_plng_dst_pf_inv_age_lnk
)

SELECT * FROM final
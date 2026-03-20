{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sply_plng_disti_prdct_lnk', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_SPLY_PLNG_DISTI_PRDCT_LNK',
        'target_table': 'W_SPLY_PLNG_DISTI_PRDCT_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.995453+00:00'
    }
) }}

WITH 

source_st_xxcmf_sp_disti_item_attri AS (
    SELECT
        distributor_master_name,
        product_key,
        publication_dt,
        disti_product_plng_segment_cd,
        dist_prd_whlsl_prc_lst_usd_amt,
        dist_prd_trnst_days_settng_cnt,
        dist_prd_weeks_of_sply_tgt_pct,
        creation_date,
        created_by
    FROM {{ source('raw', 'st_xxcmf_sp_disti_item_attri') }}
),

transformed_exptrans AS (
    SELECT
    distributor_master_name,
    product_key,
    publication_dt,
    disti_product_plng_segment_cd,
    dist_prd_whlsl_prc_lst_usd_amt,
    dist_prd_trnst_days_settng_cnt,
    dist_prd_weeks_of_sply_tgt_pct,
    creation_date,
    created_by,
    'I' AS action_code,
    'I' AS dml_type
    FROM source_st_xxcmf_sp_disti_item_attri
),

final AS (
    SELECT
        bk_distributor_master_name,
        product_key,
        bk_publication_dt,
        disti_product_plng_segment_cd,
        dist_prd_whlsl_prc_lst_usd_amt,
        dist_prd_trnst_days_settng_cnt,
        dist_prd_weeks_of_sply_tgt_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exptrans
)

SELECT * FROM final
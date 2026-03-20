{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_disti_prmtn_cmrs_prdct_split', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_DISTI_PRMTN_CMRS_PRDCT_SPLIT',
        'target_table': 'W_DISTI_PRMTN_CMRS_PRDCT_SPLIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.821016+00:00'
    }
) }}

WITH 

source_st_dca_promo_product_split AS (
    SELECT
        promo_prod_split_id,
        promo_id,
        promo_prod_detail_id,
        sub_promo_type,
        sub_promo_number,
        split_factor,
        required_product,
        split_quantity,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        it_comments,
        sub_promo_desc,
        batch_id,
        action_cd,
        create_datetime
    FROM {{ source('raw', 'st_dca_promo_product_split') }}
),

transformed_exp_w_disti_prmtn_cmrs_prdct_split AS (
    SELECT
    bk_cmrs_part_num,
    bk_promotion_num,
    bk_promotion_revision_num_int,
    bk_promotion_type_cd,
    bk_split_promotion_type_cd,
    bk_split_prmtn_revsion_num_int,
    bk_split_promotion_num,
    sub_promotion_num,
    sub_promotion_type_cd,
    sub_promotion_descr,
    split_qty,
    split_factor_pct,
    required_product_flg,
    edw_create_dtm,
    edw_create_user,
    edw_update_dtm,
    edw_update_user,
    'I' AS action_code,
    'I' AS dml_type
    FROM source_st_dca_promo_product_split
),

final AS (
    SELECT
        bk_cmrs_part_num,
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        bk_split_promotion_num,
        bk_split_prmtn_revsion_num_int,
        bk_split_promotion_type_cd,
        sub_promotion_num,
        sub_promotion_type_cd,
        sub_promotion_descr,
        split_qty,
        split_factor_pct,
        required_product_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exp_w_disti_prmtn_cmrs_prdct_split
)

SELECT * FROM final
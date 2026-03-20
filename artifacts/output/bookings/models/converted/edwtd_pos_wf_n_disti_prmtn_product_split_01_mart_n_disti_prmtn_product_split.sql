{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_disti_prmtn_product_split', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DISTI_PRMTN_PRODUCT_SPLIT',
        'target_table': 'N_DISTI_PRMTN_PRODUCT_SPLIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.765652+00:00'
    }
) }}

WITH 

source_w_disti_prmtn_product_split AS (
    SELECT
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        bk_split_promotion_type_cd,
        bk_split_prmtn_revsion_num_int,
        bk_split_promotion_num,
        product_key,
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
    FROM {{ source('raw', 'w_disti_prmtn_product_split') }}
),

final AS (
    SELECT
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        bk_split_promotion_type_cd,
        bk_split_prmtn_revsion_num_int,
        bk_split_promotion_num,
        product_key,
        sub_promotion_num,
        sub_promotion_type_cd,
        sub_promotion_descr,
        split_qty,
        split_factor_pct,
        required_product_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_disti_prmtn_product_split
)

SELECT * FROM final
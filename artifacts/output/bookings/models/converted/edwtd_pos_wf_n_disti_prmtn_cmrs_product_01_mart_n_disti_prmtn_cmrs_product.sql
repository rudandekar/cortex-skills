{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_disti_prmtn_cmrs_product', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DISTI_PRMTN_CMRS_PRODUCT',
        'target_table': 'N_DISTI_PRMTN_CMRS_PRODUCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.613358+00:00'
    }
) }}

WITH 

source_w_disti_prmtn_cmrs_product AS (
    SELECT
        bk_cmrs_part_num,
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        detail_type_cd,
        product_start_dt,
        product_end_dt,
        discount_qty,
        max_allowed_qty,
        calculation_criteria_name,
        new_price,
        old_price,
        discount_typ,
        product_configuration_txt,
        sk_promo_prod_detail_id_int,
        ru_discount_pct,
        ru_dollar_discnt_per_unit_amt,
        ru_max_dollar_discount_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_disti_prmtn_cmrs_product') }}
),

final AS (
    SELECT
        bk_cmrs_part_num,
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        detail_type_cd,
        product_start_dt,
        product_end_dt,
        discount_qty,
        max_allowed_qty,
        calculation_criteria_name,
        new_price,
        old_price,
        discount_typ,
        product_configuration_txt,
        sk_promo_prod_detail_id_int,
        ru_discount_pct,
        ru_dollar_discnt_per_unit_amt,
        ru_max_dollar_discount_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        source_derived_part_num
    FROM source_w_disti_prmtn_cmrs_product
)

SELECT * FROM final
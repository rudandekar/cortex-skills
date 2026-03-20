{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_disti_prmtn_cmrs_prdct_famly', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_DISTI_PRMTN_CMRS_PRDCT_FAMLY',
        'target_table': 'W_DISTI_PRMTN_CMRS_PRDCT_FAMLY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.356129+00:00'
    }
) }}

WITH 

source_st_dca_promo_product_details AS (
    SELECT
        promo_id,
        promo_prod_detail_id,
        detail_type,
        part_number,
        product_family,
        product_config,
        discount_type,
        quantity,
        dollar_discount_per_unit,
        max_dollar_discount,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        old_price,
        new_price,
        required_product,
        prod_start_date,
        prod_end_date,
        cmrs_product_family,
        it_comments,
        max_allowed_quantity,
        discount_percent,
        calculation_criteria,
        promo_max_allowed_quantity,
        promo_max_dollar_discount,
        batch_id,
        action_cd,
        create_datetime
    FROM {{ source('raw', 'st_dca_promo_product_details') }}
),

transformed_exp_w_disti_prmtn_cmrs_prdct_famly AS (
    SELECT
    bk_cmrs_product_family_id,
    bk_promotion_num,
    bk_promotion_revision_num_int,
    bk_promotion_type_cd,
    detail_type_cd,
    included_cmrs_prdct_famly_role,
    ru_product_start_dt,
    ru_product_end_dt,
    ru_discount_qty,
    ru_discount_pct,
    ru_calculation_criteria_name,
    edw_create_dtm,
    edw_create_user,
    edw_update_dtm,
    edw_update_user,
    'I' AS action_code,
    'I' AS dml_type
    FROM source_st_dca_promo_product_details
),

final AS (
    SELECT
        bk_cmrs_product_family_id,
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        detail_type_cd,
        included_cmrs_prdct_famly_role,
        ru_product_start_dt,
        ru_product_end_dt,
        ru_discount_qty,
        ru_discount_pct,
        ru_calculation_criteria_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exp_w_disti_prmtn_cmrs_prdct_famly
)

SELECT * FROM final
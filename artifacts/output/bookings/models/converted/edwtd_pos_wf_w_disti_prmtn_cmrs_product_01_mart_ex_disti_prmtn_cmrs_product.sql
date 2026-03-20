{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_disti_prmtn_cmrs_product', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_DISTI_PRMTN_CMRS_PRODUCT',
        'target_table': 'EX_DISTI_PRMTN_CMRS_PRODUCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.093817+00:00'
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

transformed_exp_w_disti_prmtn_cmrs_product AS (
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
    sk_promo_prod_detail_id_in,
    ru_discount_pct,
    ru_dollar_discnt_per_unit_int,
    ru_max_dollar_discount_amt,
    edw_create_dtm,
    edw_create_user,
    edw_update_dtm,
    edw_update_user,
    derived_part_number,
    'I' AS action_code,
    'I' AS dml_type
    FROM source_st_dca_promo_product_details
),

final AS (
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
        create_datetime,
        exception_type,
        derived_part_number
    FROM transformed_exp_w_disti_prmtn_cmrs_product
)

SELECT * FROM final
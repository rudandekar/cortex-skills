{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_disti_prmtn_cust_country', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_DISTI_PRMTN_CUST_COUNTRY',
        'target_table': 'W_DISTI_PRMTN_CUST_COUNTRY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.634718+00:00'
    }
) }}

WITH 

source_st_dca_promo_location_details AS (
    SELECT
        promo_id,
        promo_loc_detail_id,
        location_type,
        whose_location,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        it_comments,
        location_value,
        batch_id,
        action_cd,
        create_datetime
    FROM {{ source('raw', 'st_dca_promo_location_details') }}
),

transformed_exp_w_disti_prmtn_cust_country AS (
    SELECT
    bk_promotion_num,
    bk_promotion_revision_num_int,
    bk_promotion_type_cd,
    bk_disti_prmtn_cust_type_cd,
    bk_iso_country_cd,
    edw_create_dtm,
    edw_create_user,
    edw_update_dtm,
    edw_update_user,
    'I' AS action_code,
    'I' AS dml_type
    FROM source_st_dca_promo_location_details
),

final AS (
    SELECT
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        bk_disti_prmtn_cust_type_cd,
        bk_iso_country_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exp_w_disti_prmtn_cust_country
)

SELECT * FROM final
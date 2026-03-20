{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_disti_prmtn_cmrs_prdct_famly', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DISTI_PRMTN_CMRS_PRDCT_FAMLY',
        'target_table': 'N_DISTI_PRMTN_CMRS_PRDCT_FAMLY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.485597+00:00'
    }
) }}

WITH 

source_w_disti_prmtn_cmrs_prdct_famly AS (
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
    FROM {{ source('raw', 'w_disti_prmtn_cmrs_prdct_famly') }}
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
        edw_update_user
    FROM source_w_disti_prmtn_cmrs_prdct_famly
)

SELECT * FROM final
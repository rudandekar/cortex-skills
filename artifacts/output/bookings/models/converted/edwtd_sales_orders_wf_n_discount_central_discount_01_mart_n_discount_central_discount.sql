{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_discount_central_discount', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DISCOUNT_CENTRAL_DISCOUNT',
        'target_table': 'N_DISCOUNT_CENTRAL_DISCOUNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.900806+00:00'
    }
) }}

WITH 

source_w_discount_central_discount AS (
    SELECT
        bk_dscnt_central_dscnt_id_int,
        bk_dsc_cntrl_dsc_vrsn_num_int,
        discount_cd,
        discount_name,
        discount_product_type_cd,
        discount_status_cd,
        pricing_scenario_type_cd,
        source_deleted_flg,
        discount_type_cd,
        sales_path_cd,
        crtd_by_cisco_worker_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_discount_central_discount') }}
),

final AS (
    SELECT
        bk_dscnt_central_dscnt_id_int,
        bk_dsc_cntrl_dsc_vrsn_num_int,
        discount_cd,
        discount_name,
        discount_product_type_cd,
        discount_status_cd,
        pricing_scenario_type_cd,
        source_deleted_flg,
        discount_type_cd,
        sales_path_cd,
        crtd_by_cisco_worker_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_discount_central_discount
)

SELECT * FROM final
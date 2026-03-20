{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_discount_central_discount', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DISCOUNT_CENTRAL_DISCOUNT',
        'target_table': 'W_DISCOUNT_CENTRAL_DISCOUNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.742058+00:00'
    }
) }}

WITH 

source_st_cpd_std_disc_vw AS (
    SELECT
        batch_id,
        disc_header_id,
        version_number,
        effective_start_date,
        effective_end_date,
        disc_type,
        code,
        name,
        status,
        sales_path,
        erp_pricelist_id,
        scenario,
        std_disc_type,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cpd_std_disc_vw') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_cpd_std_disc_vw
)

SELECT * FROM final
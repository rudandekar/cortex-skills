{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_apsp_royalty_product', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_APSP_ROYALTY_PRODUCT',
        'target_table': 'N_APSP_ROYALTY_PRODUCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.273704+00:00'
    }
) }}

WITH 

source_n_apsp_royalty_product AS (
    SELECT
        apsp_royalty_product_key,
        top_product_key,
        royalty_model_index_num,
        royalty_multiplier_int,
        record_type,
        net_acctng_treatment_flg,
        nat_offering_name,
        ru_sw_assembly_prdt_key,
        ru_commercial_sw_itm_prdt_key,
        ru_agremnt_part_prdt_key,
        ru_agreement_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_apsp_royalty_product') }}
),

final AS (
    SELECT
        apsp_royalty_product_key,
        top_product_key,
        royalty_model_index_num,
        royalty_multiplier_int,
        record_type,
        net_acctng_treatment_flg,
        nat_offering_name,
        ru_sw_assembly_prdt_key,
        ru_commercial_sw_itm_prdt_key,
        ru_agremnt_part_prdt_key,
        ru_agreement_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_apsp_royalty_product
)

SELECT * FROM final
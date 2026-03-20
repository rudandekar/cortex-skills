{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_apsp_royalty_product', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_APSP_ROYALTY_PRODUCT',
        'target_table': 'W_APSP_ROYALTY_PRODUCT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.893460+00:00'
    }
) }}

WITH 

source_w_apsp_royalty_product AS (
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
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_apsp_royalty_product') }}
),

source_ex_refadm_brm_royalty_pids AS (
    SELECT
        top_pid,
        sw_assembly,
        commercial_sw_item,
        agreement_part,
        royalty_index,
        multiplier,
        record_type,
        nat_treatment,
        nat_offering_name,
        product_family,
        n_attribute1,
        n_attribute2,
        n_attribute3,
        n_attribute4,
        n_attribute5,
        v_attribute1,
        v_attribute2,
        v_attribute3,
        v_attribute4,
        v_attribute5,
        d_attribute1,
        d_attribute2,
        d_attribute3,
        d_attribute4,
        d_attribute5,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        exception_type
    FROM {{ source('raw', 'ex_refadm_brm_royalty_pids') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_ex_refadm_brm_royalty_pids
)

SELECT * FROM final
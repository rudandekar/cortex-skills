{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_rw_cq_qute_ln_cncs_kfka_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_RW_CQ_QUTE_LN_CNCS_KFKA_RNWL',
        'target_table': 'EL_INT_RW_CQ_QUTE_LN_CNCS_KFKA_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.917217+00:00'
    }
) }}

WITH 

source_el_int_rw_cq_qute_ln_cncs_kfka_rnwl AS (
    SELECT
        object_id,
        appr_ns_discount_pctg,
        disc_percentage,
        disc_amount,
        disc_method,
        promotion_code,
        promotion_name,
        operand_per_quantity,
        disc_name,
        adjusted_amount,
        creation_dt,
        quote_line_object_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        instance_id,
        concession_id,
        discount_group,
        quote_id,
        ext_discount_amount,
        unrounded_ext_amount
    FROM {{ source('raw', 'el_int_rw_cq_qute_ln_cncs_kfka_rnwl') }}
),

final AS (
    SELECT
        object_id,
        appr_ns_discount_pctg,
        disc_percentage,
        disc_amount,
        disc_method,
        promotion_code,
        promotion_name,
        operand_per_quantity,
        disc_name,
        adjusted_amount,
        creation_dt,
        quote_line_object_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        instance_id,
        concession_id,
        discount_group,
        quote_id,
        ext_discount_amount,
        unrounded_ext_amount
    FROM source_el_int_rw_cq_qute_ln_cncs_kfka_rnwl
)

SELECT * FROM final
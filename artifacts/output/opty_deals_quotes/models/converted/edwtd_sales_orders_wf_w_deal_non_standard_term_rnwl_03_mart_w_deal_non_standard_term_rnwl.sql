{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_non_standard_term_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_NON_STANDARD_TERM_RNWL',
        'target_table': 'W_DEAL_NON_STANDARD_TERM_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.908969+00:00'
    }
) }}

WITH 

source_st_int_raw_cq_dealnsterm_vw_rnwl AS (
    SELECT
        object_id,
        nsterm_id,
        nsterm_name,
        deal_object_id,
        created_date,
        created_by,
        updated_date,
        updated_by,
        as_subscription,
        as_transaction,
        as_fixed,
        tss_core,
        ros
    FROM {{ source('raw', 'st_int_raw_cq_dealnsterm_vw_rnwl') }}
),

source_ex_int_raw_cq_dealnsterm_vw_rnwl AS (
    SELECT
        object_id,
        nsterm_id,
        nsterm_name,
        deal_object_id,
        created_date,
        created_by,
        updated_date,
        updated_by,
        exception_type,
        as_subscription,
        as_transaction,
        as_fixed,
        tss_core,
        ros
    FROM {{ source('raw', 'ex_int_raw_cq_dealnsterm_vw_rnwl') }}
),

final AS (
    SELECT
        bk_non_standard_term_name,
        bk_deal_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        as_subscription_flg,
        as_transaction_flg,
        as_fixed_flg,
        tss_core_flg,
        ros_flg,
        action_code,
        dml_type
    FROM source_ex_int_raw_cq_dealnsterm_vw_rnwl
)

SELECT * FROM final
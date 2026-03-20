{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_w_deal_non_standard_term_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_NON_STANDARD_TERM_RNWL',
        'target_table': 'EX_INT_RAW_CQ_DEALNSTERM_VW_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.992150+00:00'
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
    FROM source_ex_int_raw_cq_dealnsterm_vw_rnwl
)

SELECT * FROM final
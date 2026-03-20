{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_w_deal_non_standard_term', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_NON_STANDARD_TERM',
        'target_table': 'ST_INT_RAW_CQ_DEALNSTERM_VW',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.990789+00:00'
    }
) }}

WITH 

source_ex_int_raw_cq_dealnsterm_vw AS (
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
    FROM {{ source('raw', 'ex_int_raw_cq_dealnsterm_vw') }}
),

source_st_int_raw_cq_dealnsterm_vw AS (
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
    FROM {{ source('raw', 'st_int_raw_cq_dealnsterm_vw') }}
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
        as_subscription,
        as_transaction,
        as_fixed,
        tss_core,
        ros
    FROM source_st_int_raw_cq_dealnsterm_vw
)

SELECT * FROM final
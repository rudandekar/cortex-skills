{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dcv_cq_qustn_respns_oem', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_DCV_CQ_QUSTN_RESPNS_OEM',
        'target_table': 'ST_DCV_CQ_QTN_RSPNS_OEM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.340297+00:00'
    }
) }}

WITH 

source_ff_dcv_cq_qtn_rspns_oem AS (
    SELECT
        deal_object_id,
        question_id,
        value_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_dcv_cq_qtn_rspns_oem') }}
),

final AS (
    SELECT
        deal_object_id,
        question_id,
        value_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        batch_id,
        create_datetime,
        action_code
    FROM source_ff_dcv_cq_qtn_rspns_oem
)

SELECT * FROM final
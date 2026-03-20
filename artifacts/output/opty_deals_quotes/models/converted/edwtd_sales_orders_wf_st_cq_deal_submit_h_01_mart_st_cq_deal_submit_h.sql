{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cq_deal_submit_h', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CQ_DEAL_SUBMIT_H',
        'target_table': 'ST_CQ_DEAL_SUBMIT_H',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.950116+00:00'
    }
) }}

WITH 

source_st_cq_deal_submit_h AS (
    SELECT
        batch_id,
        deal_object_id,
        opty_number,
        submit_date,
        submit_number,
        submit_by,
        appr_route_history,
        reason_for_approval,
        product_family,
        sotd_reason,
        comments,
        pricing_band,
        final_band,
        deal_approval,
        create_datetime,
        action_code,
        ss_code
    FROM {{ source('raw', 'st_cq_deal_submit_h') }}
),

final AS (
    SELECT
        batch_id,
        deal_object_id,
        opty_number,
        submit_date,
        submit_number,
        submit_by,
        appr_route_history,
        reason_for_approval,
        product_family,
        sotd_reason,
        comments,
        pricing_band,
        final_band,
        deal_approval,
        create_datetime,
        action_code,
        ss_code
    FROM source_st_cq_deal_submit_h
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cq_deal_pf_band_rpt', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CQ_DEAL_PF_BAND_RPT',
        'target_table': 'st_cq_deal_pf_band_rpt',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.905947+00:00'
    }
) }}

WITH 

source_ff_cq_deal_pf_band_rpt AS (
    SELECT
        batch_id,
        opty_number,
        deal_object_id,
        product_family,
        pf_discount_rule_id,
        pf_discount_band,
        created_on,
        last_run_date,
        deal_component,
        pr_ser_category,
        final_component_band,
        stringent_pf,
        deal_approval_band,
        ext_list_price_gpl,
        extended_list_price,
        extended_net_price,
        pf_approval_rule_id,
        pf_approval_band,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_cq_deal_pf_band_rpt') }}
),

final AS (
    SELECT
        batch_id,
        opty_number,
        deal_object_id,
        product_family,
        pf_discount_rule_id,
        pf_discount_band,
        created_on,
        last_run_date,
        deal_component,
        pr_ser_category,
        final_component_band,
        stringent_pf,
        deal_approval_band,
        ext_list_price_gpl,
        extended_list_price,
        extended_net_price,
        pf_approval_rule_id,
        pf_approval_band,
        create_datetime,
        action_code
    FROM source_ff_cq_deal_pf_band_rpt
)

SELECT * FROM final
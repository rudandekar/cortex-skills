{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cg_meraki_dist_all_rtbl_oa', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_CG_MERAKI_DIST_ALL_RTBL_OA',
        'target_table': 'WI_CG_MERAKI_DIST_ALL_INCR_OA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.213868+00:00'
    }
) }}

WITH 

source_wi_cg_meraki_dist_all_rtbl_oa AS (
    SELECT
        ar_trx_key,
        ar_trx_line_key,
        sales_order_line_key,
        dv_bundle_prdt_key,
        product_key,
        credit_debit_type,
        gl_dt,
        gl_posted_dt,
        credit_general_ledger_acct_key,
        debit_general_ledger_acct_key,
        sequence_id,
        acct_class_cd,
        accounting_rule_name,
        application_name,
        currency,
        instance_name,
        item_name,
        ledger_id,
        offr_attr_id_int,
        oa_percentage,
        record_offset,
        operating_unit,
        org_id,
        quantity,
        source,
        sub_sku_amount,
        sub_sku_accounted_amount,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM {{ source('raw', 'wi_cg_meraki_dist_all_rtbl_oa') }}
),

source_wi_cg_meraki_dist_all_incr_oa AS (
    SELECT
        fid,
        accounting_rule_name,
        application_name,
        currency,
        customer_trx_line_id,
        instance_name,
        inventory_item_id,
        item_name,
        kafka_topic_name,
        ledger_id,
        oa_attrib_id,
        oa_percentage,
        record_offset,
        operating_unit,
        order_line_id,
        org_id,
        quantity,
        source,
        sub_sku_amount,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        valid_flg
    FROM {{ source('raw', 'wi_cg_meraki_dist_all_incr_oa') }}
),

final AS (
    SELECT
        fid,
        accounting_rule_name,
        application_name,
        currency,
        customer_trx_line_id,
        instance_name,
        inventory_item_id,
        item_name,
        kafka_topic_name,
        ledger_id,
        oa_attrib_id,
        oa_percentage,
        record_offset,
        operating_unit,
        order_line_id,
        org_id,
        quantity,
        source,
        sub_sku_amount,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        valid_flg
    FROM source_wi_cg_meraki_dist_all_incr_oa
)

SELECT * FROM final
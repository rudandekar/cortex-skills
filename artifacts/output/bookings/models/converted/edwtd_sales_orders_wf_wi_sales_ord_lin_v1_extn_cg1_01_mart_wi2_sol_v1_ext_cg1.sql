{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_ord_lin_v1_extn_cg1', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_ORD_LIN_V1_EXTN_CG1',
        'target_table': 'WI2_SOL_V1_EXT_CG1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.520723+00:00'
    }
) }}

WITH 

source_wi2_sol_v1_ext_cg1 AS (
    SELECT
        sales_order_line_key,
        service_entitlement_status,
        line_id,
        global_name,
        quote_number,
        taa,
        cust_trx_reference_num,
        source_commit_time,
        bundle_line_id,
        clean_config_flag,
        service_offset_days,
        shipping_route_code,
        invoice_eligibility_event_cd,
        edw_create_user,
        edw_create_datetime,
        original_promise_date,
        legacy_shipment_priority_code,
        reseller_bill_to_cust_key,
        pak_delivery_pref,
        original_promised_deliver_date,
        smt_line_id,
        solution_identifier,
        future_invoice_release_date,
        requested_contract_number,
        src_drvd_cust_rqstd_shpmnt_dt,
        mfg_po_org_id,
        corp_dev_deal_name,
        corp_dev_deal_type_name,
        ela_buying_program_name,
        in_country_routing_cd,
        stock_replenishment,
        periodic_bllng_split_sol_key,
        contract_grp_sol_key,
        orig_mfg_po_org_id,
        location_cd,
        distributor_ship_center_loc_cd
    FROM {{ source('raw', 'wi2_sol_v1_ext_cg1') }}
),

final AS (
    SELECT
        sales_order_line_key,
        service_entitlement_status,
        line_id,
        global_name,
        quote_number,
        taa,
        cust_trx_reference_num,
        source_commit_time,
        bundle_line_id,
        clean_config_flag,
        service_offset_days,
        shipping_route_code,
        invoice_eligibility_event_cd,
        edw_create_user,
        edw_create_datetime,
        original_promise_date,
        legacy_shipment_priority_code,
        reseller_bill_to_cust_key,
        pak_delivery_pref,
        original_promised_deliver_date,
        smt_line_id,
        solution_identifier,
        future_invoice_release_date,
        requested_contract_number,
        src_drvd_cust_rqstd_shpmnt_dt,
        mfg_po_org_id,
        corp_dev_deal_name,
        corp_dev_deal_type_name,
        ela_buying_program_name,
        in_country_routing_cd,
        stock_replenishment,
        periodic_bllng_split_sol_key,
        contract_grp_sol_key,
        orig_mfg_po_org_id,
        location_cd,
        distributor_ship_center_loc_cd
    FROM source_wi2_sol_v1_ext_cg1
)

SELECT * FROM final
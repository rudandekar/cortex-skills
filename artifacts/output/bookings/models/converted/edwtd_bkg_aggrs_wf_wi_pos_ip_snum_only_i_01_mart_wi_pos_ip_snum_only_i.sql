{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pos_ip_snum_only_i', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_POS_IP_SNUM_ONLY_I',
        'target_table': 'WI_POS_IP_SNUM_ONLY_I',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.718411+00:00'
    }
) }}

WITH 

source_wi_pos_ip_snum_only_i AS (
    SELECT
        du_bk_serial_num,
        ip_goods_product_key,
        installation_dt,
        du_installation_qty,
        instance_status_cd,
        source_order_num,
        dv_rma_flg,
        du_warranty_cntrct_ln_end_dt,
        installed_customer_party_key,
        ip_ship_to_customer_key,
        ip_bill_to_customer_key,
        ip_key,
        ip_customer_po_num,
        ip_customer_po_type_cd,
        ip_shipment_confirmed_dtm
    FROM {{ source('raw', 'wi_pos_ip_snum_only_i') }}
),

final AS (
    SELECT
        du_bk_serial_num,
        ip_goods_product_key,
        installation_dt,
        du_installation_qty,
        instance_status_cd,
        source_order_num,
        dv_rma_flg,
        du_warranty_cntrct_ln_end_dt,
        installed_customer_party_key,
        ip_ship_to_customer_key,
        ip_bill_to_customer_key,
        ip_key,
        ip_customer_po_num,
        ip_customer_po_type_cd,
        ip_shipment_confirmed_dtm
    FROM source_wi_pos_ip_snum_only_i
)

SELECT * FROM final
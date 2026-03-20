{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_drvd_nrt_dbkg_trx_recompute', 'batch', 'edwtd_ncrnrt_bkg'],
    meta={
        'source_workflow': 'wf_m_WI_DRVD_NRT_DBKG_TRX_RECOMPUTE',
        'target_table': 'WI_DRVD_NRT_DBKG_TRX_RECOMPUTE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.806219+00:00'
    }
) }}

WITH 

source_wi_drvd_nrt_debkg_tx_recmp_int AS (
    SELECT
        drvd_ncr_bkg_trx_key,
        sales_order_line_key,
        sales_order_key,
        process_date,
        dd_item_type_code_flag,
        dd_rma_flag,
        dd_international_demo_flag,
        dd_replacement_demo_flag,
        dd_revenue_flag,
        dd_overlay_flag,
        dd_salesrep_flag,
        dd_ic_revenue_flag,
        dd_charges_flag,
        dd_misc_flag,
        dd_acquisition_flag,
        dd_service_flag,
        dd_rev_rec_flag,
        orig_rev_rec_flag,
        edw_create_datetime,
        edw_create_user
    FROM {{ source('raw', 'wi_drvd_nrt_debkg_tx_recmp_int') }}
),

final AS (
    SELECT
        drvd_ncr_bkg_trx_key,
        sales_order_line_key,
        sales_order_key,
        process_date,
        dd_item_type_code_flag,
        dd_rma_flag,
        dd_international_demo_flag,
        dd_replacement_demo_flag,
        dd_revenue_flag,
        dd_overlay_flag,
        dd_salesrep_flag,
        dd_ic_revenue_flag,
        dd_charges_flag,
        dd_misc_flag,
        dd_acquisition_flag,
        dd_service_flag,
        dd_rev_rec_flag,
        orig_rev_rec_flag,
        edw_create_datetime,
        edw_create_user,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        dv_sales_order_line_key,
        dv_product_key
    FROM source_wi_drvd_nrt_debkg_tx_recmp_int
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_drvd_ncr_bkg_trx_recomp', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DRVD_NCR_BKG_TRX_RECOMP',
        'target_table': 'WI_DRVD_NCR_BKG_TRX_RECOMPUTE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.955445+00:00'
    }
) }}

WITH 

source_wi_drvd_ncr_bkg_trx_recomp_int AS (
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
    FROM {{ source('raw', 'wi_drvd_ncr_bkg_trx_recomp_int') }}
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
        edw_create_user
    FROM source_wi_drvd_ncr_bkg_trx_recomp_int
)

SELECT * FROM final
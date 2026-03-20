{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sls_chnl_mtx_ptrn_dvad_adj', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SLS_CHNL_MTX_PTRN_DVAD_ADJ',
        'target_table': 'WI_SLS_CHNL_MTX_PTRN_DVAD_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.557529+00:00'
    }
) }}

WITH 

source_wi_sls_chnl_mtx_ptrn_dvad_adj AS (
    SELECT
        sold_to_customer_key,
        end_customer_key,
        sales_order_key,
        ide_ifc_ide_reported_deal_id,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sls_chnl_mtx_ptrn_dvad_adj') }}
),

source_wi_chnl_ptrn_bkg_dvad_sls_adj AS (
    SELECT
        sold_to_customer_key,
        end_customer_key,
        process_date,
        sales_order_key,
        ide_ifc_ide_reported_deal_id,
        manual_trx_key,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_chnl_ptrn_bkg_dvad_sls_adj') }}
),

final AS (
    SELECT
        sold_to_customer_key,
        end_customer_key,
        sales_order_key,
        ide_ifc_ide_reported_deal_id,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM source_wi_chnl_ptrn_bkg_dvad_sls_adj
)

SELECT * FROM final
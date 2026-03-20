{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_new_service_prediction', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_NEW_SERVICE_PREDICTION',
        'target_table': 'N_DEAL_NEW_SERVICE_PREDICTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.517135+00:00'
    }
) }}

WITH 

source_n_deal_new_service_prediction AS (
    SELECT
        sales_territory_key,
        deal_id,
        service_category_name,
        expected_service_bkg_usd_amt,
        new_services_prediction_pct,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'n_deal_new_service_prediction') }}
),

final AS (
    SELECT
        sales_territory_key,
        deal_id,
        service_category_name,
        expected_service_bkg_usd_amt,
        new_services_prediction_pct,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_n_deal_new_service_prediction
)

SELECT * FROM final
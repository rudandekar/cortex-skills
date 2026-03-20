{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ccm_tac_id_sales_terr_key', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CCM_TAC_ID_SALES_TERR_KEY',
        'target_table': 'WI_CCM_TAC_UNK_SK_ALLOC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.253042+00:00'
    }
) }}

WITH 

source_wi_ccm_tac_id_sales_terr_key AS (
    SELECT
        fiscal_quarter_name,
        fiscal_year_month_int,
        service_request_tac_cost_key,
        dv_end_customer_name,
        drvd_sales_territory_key,
        sa_slk_allocation_ratio,
        allocation_type,
        sr_cost
    FROM {{ source('raw', 'wi_ccm_tac_id_sales_terr_key') }}
),

source_scms_final AS (
    SELECT
        fiscal_year_month_int,
        src_rptd_sr_number_int,
        dv_end_customer_name,
        c3_cust_market_segment_name,
        c3_customer_theater_name,
        src_rptd_site_cntry_name,
        ssc_cost
    FROM {{ source('raw', 'scms_final') }}
),

source_wi_ccm_tac_unk_sk_alloc AS (
    SELECT
        fiscal_quarter_name,
        dd_external_theater_name,
        drvd_sales_territory_key,
        theatre_tac_amount,
        total_theatre_tac_amount,
        tac_slk_allocation_ratio,
        l2_sales_territory_name_code
    FROM {{ source('raw', 'wi_ccm_tac_unk_sk_alloc') }}
),

final AS (
    SELECT
        fiscal_quarter_name,
        dd_external_theater_name,
        drvd_sales_territory_key,
        theatre_tac_amount,
        total_theatre_tac_amount,
        tac_slk_allocation_ratio,
        l2_sales_territory_name_code
    FROM source_wi_ccm_tac_unk_sk_alloc
)

SELECT * FROM final
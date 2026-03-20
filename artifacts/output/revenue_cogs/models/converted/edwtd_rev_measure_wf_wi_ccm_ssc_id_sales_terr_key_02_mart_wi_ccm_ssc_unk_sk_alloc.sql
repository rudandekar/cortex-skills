{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ccm_ssc_id_sales_terr_key', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CCM_SSC_ID_SALES_TERR_KEY',
        'target_table': 'WI_CCM_SSC_UNK_SK_ALLOC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.382002+00:00'
    }
) }}

WITH 

source_wi_ccm_ssc_unk_sk_alloc AS (
    SELECT
        fiscal_quarter_name,
        dd_external_theater_name,
        drvd_sales_territory_key,
        theatre_ssc_amount,
        total_theatre_ssc_amount,
        tac_slk_allocation_ratio,
        l2_sales_territory_name_code
    FROM {{ source('raw', 'wi_ccm_ssc_unk_sk_alloc') }}
),

source_wi_ccm_ssc_id_sales_terr_key AS (
    SELECT
        fiscal_quarter_name,
        fiscal_year_month_int,
        service_request_ssc_cost_key,
        bk_repair_order_num_int,
        src_rptd_sr_number_int,
        dv_end_customer_name,
        drvd_sales_territory_key,
        sa_slk_allocation_ratio,
        allocation_type,
        rma_cost
    FROM {{ source('raw', 'wi_ccm_ssc_id_sales_terr_key') }}
),

final AS (
    SELECT
        fiscal_quarter_name,
        dd_external_theater_name,
        drvd_sales_territory_key,
        theatre_ssc_amount,
        total_theatre_ssc_amount,
        tac_slk_allocation_ratio,
        l2_sales_territory_name_code
    FROM source_wi_ccm_ssc_id_sales_terr_key
)

SELECT * FROM final
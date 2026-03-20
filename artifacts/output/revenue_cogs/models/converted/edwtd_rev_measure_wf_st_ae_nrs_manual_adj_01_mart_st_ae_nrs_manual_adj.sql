{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_nrs_manual_adj', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_AE_NRS_MANUAL_ADJ',
        'target_table': 'ST_AE_NRS_MANUAL_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.505497+00:00'
    }
) }}

WITH 

source_ff_ae_nrs_manual_adj AS (
    SELECT
        fiscal_month_id,
        sales_territory_key,
        product_id,
        measure_name,
        sub_measure_name,
        amount,
        offer_type_level_2,
        dual_gaap_flag,
        corporate_revenue_flag,
        created_by,
        create_datetime,
        updated_by,
        update_datetime
    FROM {{ source('raw', 'ff_ae_nrs_manual_adj') }}
),

final AS (
    SELECT
        fiscal_month_id,
        sales_territory_key,
        product_id,
        measure_name,
        sub_measure_name,
        amount,
        offer_type_level_2,
        dual_gaap_flag,
        corporate_revenue_flag,
        created_by,
        create_datetime,
        updated_by,
        update_datetime
    FROM source_ff_ae_nrs_manual_adj
)

SELECT * FROM final
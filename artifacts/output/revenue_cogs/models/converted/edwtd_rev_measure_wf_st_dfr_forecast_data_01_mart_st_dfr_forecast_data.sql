{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dfr_forecast_data', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_DFR_FORECAST_DATA',
        'target_table': 'ST_DFR_FORECAST_DATA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.219450+00:00'
    }
) }}

WITH 

source_ff_dfr_forecast_data AS (
    SELECT
        processed_fiscal_month_id,
        offer_type,
        product_id,
        period_type,
        fiscal_period_id,
        period_description,
        offer_component,
        offer_group,
        offer_category,
        offer_category_consolidated,
        offer_l1,
        offer_l2,
        offer_l2_customer_name,
        scenario,
        deferral_components,
        source,
        ela_flg,
        offer_ela,
        xcat_flg,
        product_subscription_flg,
        deferral,
        release_amt,
        beginning_balance,
        ending_balance,
        created_by,
        create_datetime,
        updated_by,
        update_datetime,
        business_entity,
        sub_business_entity,
        level05_theater_name,
        l3_sales_territoty_name_code,
        l3_sales_territory_descr
    FROM {{ source('raw', 'ff_dfr_forecast_data') }}
),

final AS (
    SELECT
        processed_fiscal_month_id,
        offer_type,
        product_id,
        period_type,
        fiscal_period_id,
        period_description,
        offer_component,
        offer_group,
        offer_category,
        offer_category_consolidated,
        offer_l1,
        offer_l2,
        offer_l2_customer_name,
        scenario,
        deferral_components,
        source,
        ela_flg,
        offer_ela,
        xcat_flg,
        product_subscription_flg,
        deferral,
        release_amt,
        beginning_balance,
        ending_balance,
        created_by,
        create_datetime,
        updated_by,
        update_datetime,
        business_entity,
        sub_business_entity,
        level05_theater_name,
        l3_sales_territoty_name_code,
        l3_sales_territory_descr
    FROM source_ff_dfr_forecast_data
)

SELECT * FROM final
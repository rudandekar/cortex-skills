{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_allocation', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_ST_AE_ALLOCATION',
        'target_table': 'ST_AE_ALLOCATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.186690+00:00'
    }
) }}

WITH 

source_st_ae_allocation AS (
    SELECT
        fiscal_month_id,
        sales_territory_code,
        business_entity,
        product_family,
        product_id,
        bill_to_site_use_id,
        ship_to_site_use_id,
        sold_to_customer_id,
        sub_measure_key,
        allocation_amount,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        set_of_books_id,
        company_code,
        financial_account_cd,
        sub_account_cd,
        project_cd,
        department_cd,
        financial_location_cd,
        bk_deal_id,
        gross_unbilled_accrued_rev_flg,
        net_price_flg
    FROM {{ source('raw', 'st_ae_allocation') }}
),

final AS (
    SELECT
        fiscal_month_id,
        sales_territory_code,
        business_entity,
        product_family,
        product_id,
        bill_to_site_use_id,
        ship_to_site_use_id,
        sold_to_customer_id,
        sub_measure_key,
        allocation_amount,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        set_of_books_id,
        company_code,
        financial_account_cd,
        sub_account_cd,
        project_cd,
        department_cd,
        financial_location_cd,
        bk_deal_id,
        gross_unbilled_accrued_rev_flg,
        net_price_flg
    FROM source_st_ae_allocation
)

SELECT * FROM final
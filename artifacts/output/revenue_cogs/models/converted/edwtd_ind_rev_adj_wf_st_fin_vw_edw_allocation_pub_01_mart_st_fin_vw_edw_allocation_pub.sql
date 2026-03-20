{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_fin_vw_edw_allocation_pub', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_ST_FIN_VW_EDW_ALLOCATION_PUB',
        'target_table': 'ST_FIN_VW_EDW_ALLOCATION_PUB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.739817+00:00'
    }
) }}

WITH 

source_ff_fin_vw_edw_allocation_pub AS (
    SELECT
        fiscal_month_id,
        sales_territory_code,
        business_entity,
        product_family,
        product_id,
        bill_to_site_use_id,
        ship_to_site_use_id,
        sold_to_customer_id,
        measure_id,
        sub_measure_key,
        behavior_id,
        set_of_books_id,
        company_code,
        allocation_percentage,
        sum,
        create_user,
        create_datetimestamp,
        update_user,
        update_datetimestamp,
        account_code,
        sub_account_code,
        project_code,
        department_code,
        location_code,
        incr_flag,
        deal_id,
        gross_unbilled_accrued_rev_flg,
        revenue_classification,
        recurring_flag,
        rev_clasf_rule_name,
        net_price_indicator,
        data_source_name
    FROM {{ source('raw', 'ff_fin_vw_edw_allocation_pub') }}
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
        measure_id,
        sub_measure_key,
        behavior_id,
        set_of_books_id,
        company_code,
        allocation_percentage,
        allocation_amount,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        financial_account_cd,
        sub_account_cd,
        project_cd,
        department_cd,
        financial_location_cd,
        incr_flag,
        deal_id,
        gross_unbilled_accrued_rev_flg,
        revenue_classification,
        recurring_flag,
        rev_clasf_rule_name,
        net_price_flg,
        data_source_name
    FROM source_ff_fin_vw_edw_allocation_pub
)

SELECT * FROM final
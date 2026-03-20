{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_drr_sysview_party', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_DRR_SYSVIEW_PARTY',
        'target_table': 'WI_DRR_SYSVIEW_PARTY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.432508+00:00'
    }
) }}

WITH 

source_wi_drr_sysview_party AS (
    SELECT
        customer_party_key,
        bk_sa_member_id_int,
        sales_account_group_party_key,
        link_customer_party_key,
        sls_acct_group_sales_terr_key,
        sales_rep_num,
        l6_sales_territory_name_code,
        l5_sales_territory_name_code,
        iso_country_code,
        sales_credit_split_pct,
        sales_account_group_type_cd,
        hibernation_flg,
        process_type
    FROM {{ source('raw', 'wi_drr_sysview_party') }}
),

final AS (
    SELECT
        customer_party_key,
        bk_sa_member_id_int,
        sales_account_group_party_key,
        link_customer_party_key,
        sls_acct_group_sales_terr_key,
        sales_rep_num,
        l6_sales_territory_name_code,
        l5_sales_territory_name_code,
        iso_country_code,
        sales_credit_split_pct,
        sales_account_group_type_cd,
        hibernation_flg,
        process_type,
        year_flg_int
    FROM source_wi_drr_sysview_party
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_chnl_mtx_partner_adj', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_CHNL_MTX_PARTNER_ADJ',
        'target_table': 'WI_SALES_CHNL_MTX_PARTNER_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.357790+00:00'
    }
) }}

WITH 

source_wi_sales_chnl_mtx_partner_adj AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        bill_to_customer_key,
        sold_to_party_key,
        bill_to_party_key,
        end_cust_party_key,
        partner_party_key,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sales_chnl_mtx_partner_adj') }}
),

final AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        bill_to_customer_key,
        sold_to_party_key,
        bill_to_party_key,
        end_cust_party_key,
        partner_party_key_as_is,
        partner_party_key,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM source_wi_sales_chnl_mtx_partner_adj
)

SELECT * FROM final
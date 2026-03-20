{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_so_cr_contacts_tv_cg1', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SO_CR_CONTACTS_TV_CG1',
        'target_table': 'N_SO_CR_CONTACTS_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.757423+00:00'
    }
) }}

WITH 

source_n_so_cr_contacts_tv AS (
    SELECT
        sales_order_key,
        billing_contact_locator_key,
        end_customer_contact_locator_key,
        ship_to_contact_locator_key,
        install_site_contact_locator_key,
        install_site_contact_party_key,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM {{ source('raw', 'n_so_cr_contacts_tv') }}
),

final AS (
    SELECT
        sales_order_key,
        billing_contact_locator_key,
        end_customer_contact_locator_key,
        ship_to_contact_locator_key,
        install_site_contact_locator_key,
        install_site_contact_party_key,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user
    FROM source_n_so_cr_contacts_tv
)

SELECT * FROM final
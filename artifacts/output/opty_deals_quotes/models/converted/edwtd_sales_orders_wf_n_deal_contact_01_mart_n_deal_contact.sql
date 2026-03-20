{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_contact', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_CONTACT',
        'target_table': 'N_DEAL_CONTACT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.884148+00:00'
    }
) }}

WITH 

source_n_deal_contact AS (
    SELECT
        bk_deal_id,
        bk_src_reported_contact_name,
        src_rptd_contact_type_cd,
        src_rptd_contact_subtype_cd,
        contact_cisco_worker_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_deal_contact') }}
),

final AS (
    SELECT
        bk_deal_id,
        bk_src_reported_contact_name,
        src_rptd_contact_type_cd,
        src_rptd_contact_subtype_cd,
        contact_cisco_worker_party_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_deal_contact
)

SELECT * FROM final
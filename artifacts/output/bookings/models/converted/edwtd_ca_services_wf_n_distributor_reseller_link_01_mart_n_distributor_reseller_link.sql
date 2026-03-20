{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_distributor_reseller_link', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_N_DISTRIBUTOR_RESELLER_LINK',
        'target_table': 'N_DISTRIBUTOR_RESELLER_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.585272+00:00'
    }
) }}

WITH 

source_n_distributor_reseller_link AS (
    SELECT
        dstrbtr_partner_site_party_key,
        reseller_prtnr_site_party_key,
        bk_dstrbtr_rprtd_reseller_id,
        last_reported_pos_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_distributor_reseller_link') }}
),

final AS (
    SELECT
        dstrbtr_partner_site_party_key,
        reseller_prtnr_site_party_key,
        bk_dstrbtr_rprtd_reseller_id,
        last_reported_pos_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_distributor_reseller_link
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ngccrm_rae_adj_hdr_dtls', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_EL_NGCCRM_RAE_ADJ_HDR_DTLS',
        'target_table': 'EL_NGCCRM_RAE_ADJ_HDR_DTLS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.519027+00:00'
    }
) }}

WITH 

source_el_ngccrm_rae_adj_hdr_dtls AS (
    SELECT
        adjustment_id,
        global_name,
        adjustment_category,
        adjustment_source,
        adjustment_status,
        association_type,
        line_id,
        customer_trx_line_id,
        org_id,
        source_data_key4,
        edw_create_dtm,
        edw_update_dtm,
        application_id
    FROM {{ source('raw', 'el_ngccrm_rae_adj_hdr_dtls') }}
),

final AS (
    SELECT
        adjustment_id,
        global_name,
        adjustment_category,
        adjustment_source,
        adjustment_status,
        association_type,
        line_id,
        customer_trx_line_id,
        org_id,
        source_data_key4,
        edw_create_dtm,
        edw_update_dtm,
        application_id
    FROM source_el_ngccrm_rae_adj_hdr_dtls
)

SELECT * FROM final
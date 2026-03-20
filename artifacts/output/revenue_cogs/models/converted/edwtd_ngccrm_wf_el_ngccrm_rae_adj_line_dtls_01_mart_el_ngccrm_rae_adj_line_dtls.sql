{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ngccrm_rae_adj_line_dtls', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_EL_NGCCRM_RAE_ADJ_LINE_DTLS',
        'target_table': 'EL_NGCCRM_RAE_ADJ_LINE_DTLS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.489462+00:00'
    }
) }}

WITH 

source_el_ngccrm_rae_adj_line_dtls AS (
    SELECT
        adjustment_line_id,
        global_name,
        account_class,
        customer_trx_line_id,
        event_type,
        gl_date,
        gl_posted_flag,
        line_id,
        org_id,
        edw_create_dtm,
        edw_update_dtm,
        application_id
    FROM {{ source('raw', 'el_ngccrm_rae_adj_line_dtls') }}
),

final AS (
    SELECT
        adjustment_line_id,
        global_name,
        account_class,
        customer_trx_line_id,
        event_type,
        gl_date,
        gl_posted_flag,
        line_id,
        org_id,
        edw_create_dtm,
        edw_update_dtm,
        application_id
    FROM source_el_ngccrm_rae_adj_line_dtls
)

SELECT * FROM final
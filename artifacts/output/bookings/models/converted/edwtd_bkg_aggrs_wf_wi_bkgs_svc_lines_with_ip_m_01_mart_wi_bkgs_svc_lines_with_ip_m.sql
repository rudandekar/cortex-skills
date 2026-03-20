{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_bkgs_svc_lines_with_ip_m', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_BKGS_SVC_LINES_WITH_IP_M',
        'target_table': 'WI_BKGS_SVC_LINES_WITH_IP_M',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.423504+00:00'
    }
) }}

WITH 

source_wi_bkgs_svc_lines_with_ip_m AS (
    SELECT
        bk_service_contract_num,
        bk_svc_cntrct_line_start_dtm,
        svc_cntrct_line_end_dtm,
        terminated_dtm,
        terminated_reason_short_descr,
        service_status_cd,
        ip_key
    FROM {{ source('raw', 'wi_bkgs_svc_lines_with_ip_m') }}
),

final AS (
    SELECT
        bk_service_contract_num,
        bk_svc_cntrct_line_start_dtm,
        svc_cntrct_line_end_dtm,
        terminated_dtm,
        terminated_reason_short_descr,
        service_status_cd,
        ip_key
    FROM source_wi_bkgs_svc_lines_with_ip_m
)

SELECT * FROM final
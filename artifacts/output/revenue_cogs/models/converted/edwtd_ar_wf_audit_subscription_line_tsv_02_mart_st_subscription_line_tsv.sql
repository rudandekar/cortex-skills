{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_audit_subscription_line_tsv', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_AUDIT_SUBSCRIPTION_LINE_TSV',
        'target_table': 'ST_SUBSCRIPTION_LINE_TSV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.973280+00:00'
    }
) }}

WITH 

source_audit_subscription_line_tsv AS (
    SELECT
        audit_title,
        mismatch_sum_or_cnt,
        audit_table,
        audit_date
    FROM {{ source('raw', 'audit_subscription_line_tsv') }}
),

source_st_subscription_line_tsv AS (
    SELECT
        title1,
        src_count_and_sum
    FROM {{ source('raw', 'st_subscription_line_tsv') }}
),

transformed_exptrans AS (
    SELECT
    audit_title,
    mismatch_sum_or_cnt,
    audit_table,
    audit_date,
    CASE WHEN MISMATCH_SUM_OR_CNT> 1.00,ABORT('MISMATCH_COUNT <> 0.00 in N_SUBSCRIPTION_LINE_TSV') ,MISMATCH_SUM_OR_CNT < -1.00 ,ABORT('MISMATCH_SUM < -1 in N_SUBSCRIPTION_LINE_TSV'),MISMATCH_SUM_OR_CNT) AS aud_check
    FROM source_st_subscription_line_tsv
),

final AS (
    SELECT
        title1,
        src_count_and_sum
    FROM transformed_exptrans
)

SELECT * FROM final
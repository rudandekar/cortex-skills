{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_cib_sales_hier', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_CIB_SALES_HIER',
        'target_table': 'CSF_CIB_SALES_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.114334+00:00'
    }
) }}

WITH 

source_stg_csf_cib_sales_hier AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        sh_seqid,
        supertheater,
        theater,
        consolidation,
        area,
        operation,
        region
    FROM {{ source('raw', 'stg_csf_cib_sales_hier') }}
),

source_csf_sales_hier AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        sh_seqid,
        supertheater,
        theater,
        consolidation,
        area,
        operation,
        region
    FROM {{ source('raw', 'csf_sales_hier') }}
),

transformed_exptrans AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    sh_seqid,
    supertheater,
    theater,
    consolidation,
    area,
    operation,
    region
    FROM source_csf_sales_hier
),

final AS (
    SELECT
        source_dml_type,
        ges_update_date,
        refresh_datetime,
        sh_seqid,
        supertheater,
        theater,
        consolidation,
        area,
        operation,
        region
    FROM transformed_exptrans
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_csone_hb_problem_code', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_CSONE_HB_PROBLEM_CODE',
        'target_table': 'ST_CSONE_HB_PROBLEM_CODE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.963999+00:00'
    }
) }}

WITH 

source_ff_csone_hb_problem_code AS (
    SELECT
        hb_key,
        createddate,
        id,
        isdeleted,
        lastmodifieddate,
        problem_code_description,
        problem_code_sequence_id,
        problem_code
    FROM {{ source('raw', 'ff_csone_hb_problem_code') }}
),

transformed_expr AS (
    SELECT
    hb_key,
    createddate,
    id,
    isdeleted,
    lastmodifieddate,
    problem_code_description,
    problem_code_sequence_id,
    problem_code,
    IFF(LTRIM(RTRIM(HB_KEY)) = '\N',NULL,HB_KEY) AS o_hb_key,
    IFF(LTRIM(RTRIM(CREATEDDATE)) = '\N',NULL,TO_DATE(CREATEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS o_createddate,
    IFF(LTRIM(RTRIM(ID)) = '\N',NULL,ID) AS o_id,
    IFF(LTRIM(RTRIM(ISDELETED)) = '\N',NULL,ISDELETED) AS o_isdeleted,
    IFF(LTRIM(RTRIM(LASTMODIFIEDDATE)) = '\N',NULL,TO_DATE(LASTMODIFIEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS o_lastmodifieddate,
    IFF(LTRIM(RTRIM(PROBLEM_CODE_DESCRIPTION)) = '\N',NULL,PROBLEM_CODE_DESCRIPTION) AS o_problem_code_description,
    IFF(LTRIM(RTRIM(PROBLEM_CODE_SEQUENCE_ID)) = '\N',NULL,PROBLEM_CODE_SEQUENCE_ID) AS o_problem_code_sequence_id,
    IFF(LTRIM(RTRIM(PROBLEM_CODE)) = '\N',NULL,PROBLEM_CODE) AS o_problem_code
    FROM source_ff_csone_hb_problem_code
),

final AS (
    SELECT
        hb_key,
        createddate,
        id,
        isdeleted,
        lastmodifieddate,
        problem_code_description,
        problem_code_sequence_id,
        problem_code
    FROM transformed_expr
)

SELECT * FROM final
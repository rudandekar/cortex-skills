{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_bdb_tasks', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_BDB_TASKS',
        'target_table': 'ST_BDB_TASKS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.274786+00:00'
    }
) }}

WITH 

source_ff_bdb_tasks AS (
    SELECT
        id,
        name,
        admin_labels,
        labels,
        author_uid,
        contributors,
        created_at,
        updated_at
    FROM {{ source('raw', 'ff_bdb_tasks') }}
),

transformed_exptrans AS (
    SELECT
    id,
    name,
    admin_labels,
    labels,
    author_uid,
    contributors,
    created_at,
    updated_at,
    IFF(LTRIM(RTRIM(ID)) = '\N',NULL,ID) AS o_id,
    IFF(LTRIM(RTRIM(NAME)) = '\N',NULL,NAME) AS o_name,
    IFF(LTRIM(RTRIM(ADMIN_LABELS)) = '\N',NULL,ADMIN_LABELS) AS o_admin_labels,
    IFF(LTRIM(RTRIM(LABELS)) = '\N',NULL,LABELS) AS o_labels,
    IFF(LTRIM(RTRIM(AUTHOR_UID)) = '\N',NULL,AUTHOR_UID) AS o_author_uid,
    IFF(LTRIM(RTRIM(CONTRIBUTORS)) = '\N',NULL,CONTRIBUTORS) AS o_contributors,
    IFF(LTRIM(RTRIM(CREATED_AT)) = '\N',NULL,TO_DATE(CREATED_AT,'YYYY-MM-DD HH24:MI:SS')) AS o_created_at,
    IFF(LTRIM(RTRIM(UPDATED_AT)) = '\N',NULL,TO_DATE(UPDATED_AT,'YYYY-MM-DD HH24:MI:SS')) AS o_updated_at
    FROM source_ff_bdb_tasks
),

final AS (
    SELECT
        id,
        name,
        admin_labels,
        labels,
        author_uid,
        contributors,
        created_at,
        updated_at
    FROM transformed_exptrans
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_as_cogs_proj_sol_link', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_AS_COGS_PROJ_SOL_LINK',
        'target_table': 'WI_AS_COGS_PROJ_SOL_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.422549+00:00'
    }
) }}

WITH 

source_wi_as_cogs_proj_sol_link AS (
    SELECT
        bk_as_project_cd,
        sales_order_line_key,
        sol_identifier,
        as_project_class_cd
    FROM {{ source('raw', 'wi_as_cogs_proj_sol_link') }}
),

final AS (
    SELECT
        bk_as_project_cd,
        sales_order_line_key,
        sol_identifier,
        as_project_class_cd
    FROM source_wi_as_cogs_proj_sol_link
)

SELECT * FROM final
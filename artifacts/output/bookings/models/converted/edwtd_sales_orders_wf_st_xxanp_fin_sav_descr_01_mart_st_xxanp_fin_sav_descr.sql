{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxanp_fin_sav_descr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_XXANP_FIN_SAV_DESCR',
        'target_table': 'ST_XXANP_FIN_SAV_DESCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.611108+00:00'
    }
) }}

WITH 

source_xxanp_fin_sav_descr AS (
    SELECT
        reason_cd,
        new_rstmnt_type_name
    FROM {{ source('raw', 'xxanp_fin_sav_descr') }}
),

final AS (
    SELECT
        reason_cd,
        new_rstmnt_type_name
    FROM source_xxanp_fin_sav_descr
)

SELECT * FROM final
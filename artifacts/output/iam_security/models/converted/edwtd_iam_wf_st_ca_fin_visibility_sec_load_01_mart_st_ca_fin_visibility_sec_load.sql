{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ca_fin_visibility_sec_load', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_ST_CA_FIN_VISIBILITY_SEC_LOAD',
        'target_table': 'ST_CA_FIN_VISIBILITY_SEC_LOAD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.735558+00:00'
    }
) }}

WITH 

source_ff_ca_fin_visibilty_security_load AS (
    SELECT
        user_id,
        min_level,
        biz_role_name,
        subrole_name,
        territory_code,
        role_name,
        fiscal_year,
        load_type,
        load_flag,
        sales_territory_code
    FROM {{ source('raw', 'ff_ca_fin_visibilty_security_load') }}
),

final AS (
    SELECT
        user_id,
        min_level,
        biz_role_name,
        subrole_name,
        territory_code,
        role_name,
        fiscal_year,
        load_type,
        load_flag,
        sales_territory_code
    FROM source_ff_ca_fin_visibilty_security_load
)

SELECT * FROM final
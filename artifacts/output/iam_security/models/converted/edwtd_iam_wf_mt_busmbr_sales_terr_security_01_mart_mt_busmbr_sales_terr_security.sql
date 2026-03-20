{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_busmbr_sales_terr_security', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_MT_BUSMBR_SALES_TERR_SECURITY',
        'target_table': 'MT_BUSMBR_SALES_TERR_SECURITY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.780161+00:00'
    }
) }}

WITH 

source_mt_busmbr_sales_terr_security AS (
    SELECT
        dd_cec_id,
        cec_id_fiscal_month,
        fiscal_month_name,
        user_name,
        landing_page,
        derived_role,
        special_condition_flag,
        iam_level_num_int,
        top_iam_level_num_int,
        sales_agent_flag,
        sales_territory_descr,
        sales_terr_assgnmnt_type_cd,
        sales_terr_assgnmnt_type_name,
        sales_rep_number,
        inside_sales_flag,
        business_function,
        job_title,
        job_location
    FROM {{ source('raw', 'mt_busmbr_sales_terr_security') }}
),

final AS (
    SELECT
        dd_cec_id,
        cec_id_fiscal_month,
        fiscal_month_name,
        user_name,
        landing_page,
        derived_role,
        special_condition_flag,
        iam_level_num_int,
        top_iam_level_num_int,
        sales_agent_flag,
        sales_territory_descr,
        sales_terr_assgnmnt_type_cd,
        sales_terr_assgnmnt_type_name,
        sales_rep_number,
        inside_sales_flag,
        business_function,
        job_title,
        job_location
    FROM source_mt_busmbr_sales_terr_security
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_as_cogs_proj_sol', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_AS_COGS_PROJ_SOL',
        'target_table': 'WI_AS_COGS_PROJ_SOLK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.770017+00:00'
    }
) }}

WITH 

source_wi_as_cogs_proj_no_sol AS (
    SELECT
        bk_as_project_cd,
        as_project_class_cd,
        as_project_name,
        erp_wips_customer_name,
        projet_business_model,
        as_local_deal_id_int
    FROM {{ source('raw', 'wi_as_cogs_proj_no_sol') }}
),

source_wi_as_cogs_proj_no_sol_amt AS (
    SELECT
        bk_as_project_cd,
        as_project_class_cd,
        as_project_name,
        erp_wips_customer_name,
        projet_business_model,
        current_version_flg,
        revenue_amount,
        cost
    FROM {{ source('raw', 'wi_as_cogs_proj_no_sol_amt') }}
),

source_wi_as_cogs_proj_solk AS (
    SELECT
        bk_as_project_cd,
        as_local_deal_id_int,
        sales_order_line_key,
        bk_so_number_int,
        customer_name,
        as_project_class_cd
    FROM {{ source('raw', 'wi_as_cogs_proj_solk') }}
),

final AS (
    SELECT
        bk_as_project_cd,
        as_local_deal_id_int,
        sales_order_line_key,
        bk_so_number_int,
        customer_name,
        as_project_class_cd
    FROM source_wi_as_cogs_proj_solk
)

SELECT * FROM final
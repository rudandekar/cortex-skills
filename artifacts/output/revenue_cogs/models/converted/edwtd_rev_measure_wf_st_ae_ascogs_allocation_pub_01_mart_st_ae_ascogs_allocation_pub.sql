{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_ascogs_allocation_pub', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_AE_ASCOGS_ALLOCATION_PUB',
        'target_table': 'ST_AE_ASCOGS_ALLOCATION_PUB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.023864+00:00'
    }
) }}

WITH 

source_ff_ae_ascogs_allocation_pub AS (
    SELECT
        fiscal_month_id,
        node_level05_value,
        node_level06_value,
        node_level07_value,
        node_level08_value,
        account_code,
        dept_number,
        reporting_theater,
        sales_territory_code,
        amount,
        as_cogs_triangulation_id,
        allocation_type,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        sub_measure_key,
        allocation_method,
        bk_as_project_cd
    FROM {{ source('raw', 'ff_ae_ascogs_allocation_pub') }}
),

final AS (
    SELECT
        fiscal_month_id,
        node_level05_value,
        node_level06_value,
        node_level07_value,
        node_level08_value,
        account_code,
        dept_number,
        reporting_theater,
        sales_territory_code,
        amount,
        as_cogs_triangulation_id,
        allocation_type,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        sub_measure_key,
        allocation_method,
        bk_as_project_cd
    FROM source_ff_ae_ascogs_allocation_pub
)

SELECT * FROM final
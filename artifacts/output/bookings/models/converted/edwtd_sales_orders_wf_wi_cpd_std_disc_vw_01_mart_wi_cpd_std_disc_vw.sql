{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cpd_std_disc_vw', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_CPD_STD_DISC_VW',
        'target_table': 'WI_CPD_STD_DISC_VW',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.399813+00:00'
    }
) }}

WITH 

source_ex_cpd_std_disc_vw AS (
    SELECT
        batch_id,
        disc_header_id,
        version_number,
        effective_start_date,
        effective_end_date,
        disc_type,
        code,
        name,
        status,
        sales_path,
        erp_pricelist_id,
        scenario,
        std_disc_type,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_cpd_std_disc_vw') }}
),

source_st_cpd_std_disc_vw AS (
    SELECT
        batch_id,
        disc_header_id,
        version_number,
        effective_start_date,
        effective_end_date,
        disc_type,
        code,
        name,
        status,
        sales_path,
        erp_pricelist_id,
        scenario,
        std_disc_type,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cpd_std_disc_vw') }}
),

final AS (
    SELECT
        batch_id,
        disc_header_id,
        version_number,
        effective_start_date,
        effective_end_date,
        disc_type,
        code,
        name,
        status,
        sales_path,
        erp_pricelist_id,
        scenario,
        std_disc_type,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code
    FROM source_st_cpd_std_disc_vw
)

SELECT * FROM final
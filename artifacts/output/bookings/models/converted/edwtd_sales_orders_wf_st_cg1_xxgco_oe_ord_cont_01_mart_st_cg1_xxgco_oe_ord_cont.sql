{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_xxgco_oe_ord_cont', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_XXGCO_OE_ORD_CONT',
        'target_table': 'ST_CG1_XXGCO_OE_ORD_CONT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.942698+00:00'
    }
) }}

WITH 

source_cg1_xxgco_oe_ord_cont AS (
    SELECT
        header_id,
        last_name,
        first_name,
        email_address,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        contact_type,
        telephone,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'cg1_xxgco_oe_ord_cont') }}
),

final AS (
    SELECT
        batch_id,
        header_id,
        last_name,
        first_name,
        email_address,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        contact_type,
        telephone,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        global_name
    FROM source_cg1_xxgco_oe_ord_cont
)

SELECT * FROM final
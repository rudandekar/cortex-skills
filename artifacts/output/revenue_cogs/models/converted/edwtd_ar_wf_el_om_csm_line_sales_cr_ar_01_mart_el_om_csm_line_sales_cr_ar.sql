{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_om_csm_line_sales_cr_ar', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_OM_CSM_LINE_SALES_CR_AR',
        'target_table': 'EL_OM_CSM_LINE_SALES_CR_AR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.176678+00:00'
    }
) }}

WITH 

source_st_om_csm_line_sales_cr_ar AS (
    SELECT
        batch_id,
        line_seq_id,
        source_type,
        source_header_id,
        source_line_id,
        salesrep_id,
        territory_id,
        sales_credit_type_id,
        split_percent,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_csm_line_sales_cr_ar') }}
),

final AS (
    SELECT
        line_seq_id,
        source_type,
        source_header_id,
        source_line_id,
        salesrep_id,
        territory_id,
        sales_credit_type_id,
        split_percent,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        global_name
    FROM source_st_om_csm_line_sales_cr_ar
)

SELECT * FROM final
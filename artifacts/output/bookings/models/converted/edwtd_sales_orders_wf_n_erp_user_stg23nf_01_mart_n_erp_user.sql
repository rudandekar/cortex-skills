{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_erp_user', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_ERP_USER',
        'target_table': 'N_ERP_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.435157+00:00'
    }
) }}

WITH 

source_st_otm_fnd_user AS (
    SELECT
        employee_number,
        etl_process_date,
        last_update_date,
        user_id,
        user_name
    FROM {{ source('raw', 'st_otm_fnd_user') }}
),

final AS (
    SELECT
        erp_user_name,
        cs_relationship_manager_role,
        cisco_worker_party_key,
        sr_resource_role,
        csr_resource_role,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_otm_fnd_user
)

SELECT * FROM final
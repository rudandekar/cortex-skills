{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_acqui_pid_alloc_mapping', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_ACQUI_PID_ALLOC_MAPPING',
        'target_table': 'ST_ACQUI_PID_ALLOC_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.556522+00:00'
    }
) }}

WITH 

source_ff_acqui_pid_alloc_mapping AS (
    SELECT
        fiscal_month_id,
        acquisition_name,
        acq_svc_pid,
        acq_hw_pid,
        business_entity_descr,
        sub_business_entity_descr,
        bk_prdt_allctn_clsfctn_cd,
        alloc_pct,
        create_user,
        create_datetime,
        update_user,
        update_datetime
    FROM {{ source('raw', 'ff_acqui_pid_alloc_mapping') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        acquisition_name,
        acqui_sku,
        bk_product_id,
        business_entity_descr,
        sub_business_entity_descr,
        bk_prdt_allctn_clsfctn_cd,
        pid_pct,
        update_datetime
    FROM source_ff_acqui_pid_alloc_mapping
)

SELECT * FROM final
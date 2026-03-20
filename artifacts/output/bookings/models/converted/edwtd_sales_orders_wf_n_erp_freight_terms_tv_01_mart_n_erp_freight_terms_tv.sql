{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ erp_freight_terms_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_ ERP_FREIGHT_TERMS_TV',
        'target_table': 'N_ERP_FREIGHT_TERMS_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.932425+00:00'
    }
) }}

WITH 

source_w_erp_freight_terms AS (
    SELECT
        bk_freight_terms_code,
        start_tv_date,
        end_tv_date,
        freight_terms_name,
        freight_terms_description,
        freight_terms_enabled_flag,
        freight_terms_start_active_dtm,
        freight_terms_end_active_dtm,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        edw_observation_datetime,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_erp_freight_terms') }}
),

final AS (
    SELECT
        bk_freight_terms_code,
        start_tv_date,
        end_tv_date,
        freight_terms_name,
        freight_terms_description,
        freight_terms_enabled_flag,
        freight_terms_start_active_dtm,
        freight_terms_end_active_dtm,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        edw_observation_datetime
    FROM source_w_erp_freight_terms
)

SELECT * FROM final
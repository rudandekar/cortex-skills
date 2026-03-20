{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_n_erp_fob_point_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_ERP_FOB_POINT_TV',
        'target_table': 'N_ERP_FOB_POINT_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.477034+00:00'
    }
) }}

WITH 

source_w_erp_fob_point AS (
    SELECT
        bk_fob_point_code,
        start_tv_date,
        end_tv_date,
        fob_point_name,
        fob_point_description,
        fob_point_enabled_flag,
        fob_point_start_active_dtm,
        fob_point_end_active_dtm,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        edw_observation_datetime,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_erp_fob_point') }}
),

final AS (
    SELECT
        bk_fob_point_code,
        start_tv_date,
        end_tv_date,
        fob_point_name,
        fob_point_description,
        fob_point_enabled_flag,
        fob_point_start_active_dtm,
        fob_point_end_active_dtm,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        edw_observation_datetime
    FROM source_w_erp_fob_point
)

SELECT * FROM final
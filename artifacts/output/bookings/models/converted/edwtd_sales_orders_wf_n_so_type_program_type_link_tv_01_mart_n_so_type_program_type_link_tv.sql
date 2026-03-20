{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_so_type_program_type_link_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SO_TYPE_PROGRAM_TYPE_LINK_TV',
        'target_table': 'N_SO_TYPE_PROGRAM_TYPE_LINK_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.523125+00:00'
    }
) }}

WITH 

source_w_so_type_program_type_link AS (
    SELECT
        bk_order_type_name,
        bk_so_program_type_name,
        shipment_priority_cd,
        source_deleted_flg,
        link_definition_txt,
        link_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_so_type_program_type_link') }}
),

final AS (
    SELECT
        bk_order_type_name,
        bk_so_program_type_name,
        shipment_priority_cd,
        source_deleted_flg,
        link_definition_txt,
        link_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt
    FROM source_w_so_type_program_type_link
)

SELECT * FROM final
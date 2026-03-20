{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_distributor_warehouse', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DISTRIBUTOR_WAREHOUSE',
        'target_table': 'N_DISTRIBUTOR_WAREHOUSE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.259153+00:00'
    }
) }}

WITH 

source_w_distributor_warehouse AS (
    SELECT
        bk_wips_originator_id_int,
        distributor_warehouse_num,
        disti_warehouse_descr,
        disti_warehouse_line_1_addr,
        disti_warehouse_line_2_addr,
        disti_warehouse_city_name,
        disti_warehouse_state_name,
        disti_warehouse_postal_cd,
        disti_warehouse_country_name,
        active_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_distributor_warehouse') }}
),

final AS (
    SELECT
        bk_wips_originator_id_int,
        distributor_warehouse_num,
        disti_warehouse_descr,
        disti_warehouse_line_1_addr,
        disti_warehouse_line_2_addr,
        disti_warehouse_city_name,
        disti_warehouse_state_name,
        disti_warehouse_postal_cd,
        disti_warehouse_country_name,
        active_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_distributor_warehouse
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_distributor_warehouse', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_DISTRIBUTOR_WAREHOUSE',
        'target_table': 'W_DISTRIBUTOR_WAREHOUSE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.798652+00:00'
    }
) }}

WITH 

source_st_wips_source_profile_attr AS (
    SELECT
        attribute1,
        source_profile_id,
        attribute_value,
        active_flag,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        source_name,
        territory_id,
        sales_territory_id,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        batch_id,
        action_code
    FROM {{ source('raw', 'st_wips_source_profile_attr') }}
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
        edw_update_dtm,
        action_code,
        dml_type
    FROM source_st_wips_source_profile_attr
)

SELECT * FROM final
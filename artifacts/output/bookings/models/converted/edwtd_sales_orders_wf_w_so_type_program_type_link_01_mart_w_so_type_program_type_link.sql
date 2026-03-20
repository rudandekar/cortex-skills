{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_so_type_program_type_link', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SO_TYPE_PROGRAM_TYPE_LINK',
        'target_table': 'W_SO_TYPE_PROGRAM_TYPE_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.958595+00:00'
    }
) }}

WITH 

source_st_cg1_fnd_lookup_values_so AS (
    SELECT
        lookup_type,
        lang,
        lookup_code,
        meaning,
        description,
        enabled_flag,
        start_date_active,
        end_date_active,
        created_by,
        creation_date,
        last_updated_by,
        last_update_login,
        last_update_date,
        source_lang,
        security_group_id,
        view_application_id,
        territory_code,
        attribute_category,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        tag,
        leaf_node,
        source_commit_time,
        global_name
    FROM {{ source('raw', 'st_cg1_fnd_lookup_values_so') }}
),

transformed_ex_w_so_type_program_type_link AS (
    SELECT
    bk_so_type_name,
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
    FROM source_st_cg1_fnd_lookup_values_so
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
        end_tv_dt,
        action_code,
        dml_type
    FROM transformed_ex_w_so_type_program_type_link
)

SELECT * FROM final
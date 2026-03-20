{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_om_cfi_adj_type_all', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_OM_CFI_ADJ_TYPE_ALL',
        'target_table': 'EL_OM_CFI_ADJ_TYPE_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.168158+00:00'
    }
) }}

WITH 

source_st_cg_fnd_lookup_values_ar AS (
    SELECT
        batch_id,
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
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg_fnd_lookup_values_ar') }}
),

source_st_om_cfi_adj_type_all AS (
    SELECT
        batch_id,
        adj_cat_id,
        adj_source_id,
        adj_type_desc,
        adj_type_id,
        adj_type_name,
        adj_type_type,
        allocation_method,
        attribute1,
        attribute10,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        ccrm_flag,
        channel,
        cogs_percent,
        cogs_source,
        created_by,
        creation_date,
        customer,
        db_source_name,
        dest_set_of_books_id,
        enabled_flag,
        end_date_active,
        from_clause,
        ges_update_date,
        global_name,
        gl_postable_flag,
        last_updated_by,
        last_update_date,
        last_update_login,
        notes,
        product,
        report_level_1_id,
        report_level_2_id,
        rev_percent,
        sales,
        select_clause,
        set_of_books_id,
        start_date_active,
        where_clause,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cfi_adj_type_all') }}
),

final AS (
    SELECT
        adj_type_id,
        adj_type_name,
        adj_source_id,
        adj_type_desc,
        start_date_active,
        end_date_active,
        enabled_flag,
        adj_type_type,
        global_name,
        report_level_1_id,
        report_level_2_id
    FROM source_st_om_cfi_adj_type_all
)

SELECT * FROM final
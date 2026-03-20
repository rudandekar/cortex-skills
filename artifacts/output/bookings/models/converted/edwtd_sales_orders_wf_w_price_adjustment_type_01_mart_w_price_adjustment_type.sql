{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_price_adjustment_type', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_PRICE_ADJUSTMENT_TYPE',
        'target_table': 'W_PRICE_ADJUSTMENT_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.694673+00:00'
    }
) }}

WITH 

source_st_om_fnd_lookup_values_sod AS (
    SELECT
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute_category,
        created_by,
        creation_date,
        description,
        enabled_flag,
        end_date_active,
        ges_update_date,
        global_name,
        languag,
        last_updated_by,
        last_update_date,
        last_update_login,
        lookup_code,
        lookup_type,
        meaning,
        security_group_id,
        source_lang,
        start_date_active,
        tag,
        territory_code,
        view_application_id,
        leaf_node
    FROM {{ source('raw', 'st_om_fnd_lookup_values_sod') }}
),

transformed_exptrans AS (
    SELECT
    lookup_code,
    meaning,
    global_name,
    edw_create_dtm,
    edw_create_user,
    edw_update_dtm,
    edw_update_user,
    dml_type,
    'I' AS action_code
    FROM source_st_om_fnd_lookup_values_sod
),

final AS (
    SELECT
        bk_price_adjustment_type_cd,
        price_adjustment_type_name,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exptrans
)

SELECT * FROM final
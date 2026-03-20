{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_demand_class', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_DEMAND_CLASS',
        'target_table': 'W_SALES_ORDER_DEMAND_CLASS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.239284+00:00'
    }
) }}

WITH 

source_el_fnd_lookup_values AS (
    SELECT
        lookup_type,
        lang,
        security_group_id,
        lookup_code,
        meaning,
        view_application_id,
        global_name,
        enabled_flag,
        description,
        create_datetime,
        ges_update_date,
        start_date_active,
        end_date_active,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5
    FROM {{ source('raw', 'el_fnd_lookup_values') }}
),

final AS (
    SELECT
        bk_demand_class_cd,
        demand_class_descr,
        demand_class_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_el_fnd_lookup_values
)

SELECT * FROM final
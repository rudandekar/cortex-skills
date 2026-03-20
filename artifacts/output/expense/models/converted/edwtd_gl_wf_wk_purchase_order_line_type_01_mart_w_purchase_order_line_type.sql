{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_purchase_order_line_type', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_PURCHASE_ORDER_LINE_TYPE',
        'target_table': 'W_PURCHASE_ORDER_LINE_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.667873+00:00'
    }
) }}

WITH 

source_st_mf_po_line_types_tl AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        description,
        language_code,
        last_update_date,
        last_updated_by,
        line_type,
        line_type_id,
        source_lang,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_po_line_types_tl') }}
),

transformed_exp_w_purchase_order_line_type AS (
    SELECT
    bk_purchase_order_line_type_cd,
    purchase_order_line_type_descr,
    ges_update_date,
    action_code,
    dml_type,
    purchase_basis
    FROM source_st_mf_po_line_types_tl
),

final AS (
    SELECT
        bk_purchase_order_line_type_cd,
        purchase_basis_cd,
        purchase_order_line_type_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exp_w_purchase_order_line_type
)

SELECT * FROM final
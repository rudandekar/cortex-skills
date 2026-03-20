{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fa_po_line_link', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FA_PO_LINE_LINK',
        'target_table': 'N_FA_PO_LINE_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.781660+00:00'
    }
) }}

WITH 

source_w_fa_po_line_link AS (
    SELECT
        purchase_order_line_key,
        bk_fixed_asset_num,
        bk_company_cd,
        set_of_books_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_fa_po_line_link') }}
),

final AS (
    SELECT
        purchase_order_line_key,
        bk_fixed_asset_num,
        bk_company_cd,
        set_of_books_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_fa_po_line_link
)

SELECT * FROM final
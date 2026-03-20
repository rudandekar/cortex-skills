{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fa_po_link', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FA_PO_LINK',
        'target_table': 'N_FA_PO_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.122518+00:00'
    }
) }}

WITH 

source_w_fa_po_link AS (
    SELECT
        bk_fixed_asset_num,
        bk_company_cd,
        set_of_books_key,
        purchase_order_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_fa_po_link') }}
),

final AS (
    SELECT
        bk_fixed_asset_num,
        bk_company_cd,
        set_of_books_key,
        purchase_order_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_fa_po_link
)

SELECT * FROM final
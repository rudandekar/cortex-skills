{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_move_order', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_MOVE_ORDER',
        'target_table': 'N_MOVE_ORDER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.437122+00:00'
    }
) }}

WITH 

source_w_move_order AS (
    SELECT
        bk_request_num,
        inventory_organization_key,
        sk_header_id_int,
        move_order_type_cd,
        move_order_approval_status_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_move_order') }}
),

final AS (
    SELECT
        bk_request_num,
        inventory_organization_key,
        sk_header_id_int,
        move_order_type_cd,
        move_order_approval_status_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_move_order
)

SELECT * FROM final
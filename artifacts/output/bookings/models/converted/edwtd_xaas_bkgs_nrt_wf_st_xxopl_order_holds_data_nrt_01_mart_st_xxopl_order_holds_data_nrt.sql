{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxopl_order_holds_data_nrt', 'batch', 'edwtd_xaas_bkgs_nrt'],
    meta={
        'source_workflow': 'wf_m_ST_XXOPL_ORDER_HOLDS_DATA_NRT',
        'target_table': 'ST_XXOPL_ORDER_HOLDS_DATA_NRT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.369587+00:00'
    }
) }}

WITH 

source_st_xxopl_order_holds_data_nrt AS (
    SELECT
        id,
        edw_create_dtm,
        header_id,
        order_number,
        line_id,
        line_ref_number,
        hold_id,
        hold_apply_reason,
        hold_apply_comment,
        hold_applied_date,
        hold_applied_by,
        hold_release_date,
        hold_released_by,
        hold_release_comment,
        hold_release_reason,
        released_flag,
        hold_caused_by,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        total_order_amount,
        hold_amount,
        hold_name,
        hold_source_id
    FROM {{ source('raw', 'st_xxopl_order_holds_data_nrt') }}
),

final AS (
    SELECT
        id,
        edw_create_dtm,
        header_id,
        order_number,
        line_id,
        line_ref_number,
        hold_id,
        hold_apply_reason,
        hold_apply_comment,
        hold_applied_date,
        hold_applied_by,
        hold_release_date,
        hold_released_by,
        hold_release_comment,
        hold_release_reason,
        released_flag,
        hold_caused_by,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        total_order_amount,
        hold_amount,
        hold_name,
        hold_source_id
    FROM source_st_xxopl_order_holds_data_nrt
)

SELECT * FROM final
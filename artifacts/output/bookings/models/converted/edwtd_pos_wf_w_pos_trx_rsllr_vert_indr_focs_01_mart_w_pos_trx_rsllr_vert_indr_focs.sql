{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_pos_trx_rsllr_vert_indr_focs', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_POS_TRX_RSLLR_VERT_INDR_FOCS',
        'target_table': 'W_POS_TRX_RSLLR_VERT_INDR_FOCS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.479353+00:00'
    }
) }}

WITH 

source_st_wips_pos_trans_details AS (
    SELECT
        detail_type,
        detail_value,
        pos_trans_id,
        detail_id,
        active_flag,
        serial_number_status,
        serial_number_reason_code,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        create_datetime,
        batch_id,
        action_code
    FROM {{ source('raw', 'st_wips_pos_trans_details') }}
),

final AS (
    SELECT
        bk_pos_transaction_id_int,
        bk_src_rprtd_vert_focus_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_wips_pos_trans_details
)

SELECT * FROM final
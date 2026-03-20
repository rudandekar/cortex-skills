{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_return_products_to_bts_srl', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_RETURN_PRODUCTS_TO_BTS_SRL',
        'target_table': 'N_RETURN_PRODUCTS_TO_BTS_SRL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.232819+00:00'
    }
) }}

WITH 

source_w_return_products_to_bts_srl AS (
    SELECT
        bk_serial_num,
        process_status_cd,
        create_dtm,
        dv_create_dt,
        sk_serial_id_int,
        sk_pid_line_id_int,
        sk_message_id,
        return_products_to_bts_dtl_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_return_products_to_bts_srl') }}
),

final AS (
    SELECT
        bk_serial_num,
        process_status_cd,
        create_dtm,
        dv_create_dt,
        sk_serial_id_int,
        sk_pid_line_id_int,
        sk_message_id,
        return_products_to_bts_dtl_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_return_products_to_bts_srl
)

SELECT * FROM final
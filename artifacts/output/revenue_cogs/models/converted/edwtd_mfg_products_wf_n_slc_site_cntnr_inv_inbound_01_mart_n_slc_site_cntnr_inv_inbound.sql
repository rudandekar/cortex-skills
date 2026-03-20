{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_slc_site_cntnr_inv_inbound', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_SLC_SITE_CNTNR_INV_INBOUND',
        'target_table': 'N_SLC_SITE_CNTNR_INV_INBOUND',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.289073+00:00'
    }
) }}

WITH 

source_w_slc_site_cntnr_inv_inbound AS (
    SELECT
        bk_strtgc_logistics_cntr_name,
        bk_carton_id,
        bk_response_dtm,
        sk_cycle_count_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_slc_site_cntnr_inv_inbound') }}
),

final AS (
    SELECT
        bk_strtgc_logistics_cntr_name,
        bk_carton_id,
        bk_response_dtm,
        sk_cycle_count_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_slc_site_cntnr_inv_inbound
)

SELECT * FROM final
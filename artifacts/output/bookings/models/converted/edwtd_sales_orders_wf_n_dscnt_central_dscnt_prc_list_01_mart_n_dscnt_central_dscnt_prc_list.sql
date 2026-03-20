{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_dscnt_central_dscnt_prc_list', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DSCNT_CENTRAL_DSCNT_PRC_LIST',
        'target_table': 'N_DSCNT_CENTRAL_DSCNT_PRC_LIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.152262+00:00'
    }
) }}

WITH 

source_w_dscnt_central_dscnt_prc_list AS (
    SELECT
        bk_dscnt_central_dscnt_id_int,
        bk_dsc_cntrl_dsc_vrsn_num_int,
        bk_price_list_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_dscnt_central_dscnt_prc_list') }}
),

final AS (
    SELECT
        bk_dscnt_central_dscnt_id_int,
        bk_dsc_cntrl_dsc_vrsn_num_int,
        bk_price_list_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_dscnt_central_dscnt_prc_list
)

SELECT * FROM final
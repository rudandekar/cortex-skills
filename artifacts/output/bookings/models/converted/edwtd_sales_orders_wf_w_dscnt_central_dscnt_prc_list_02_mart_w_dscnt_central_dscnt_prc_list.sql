{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_dscnt_central_dscnt_prc_list', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DSCNT_CENTRAL_DSCNT_PRC_LIST',
        'target_table': 'W_DSCNT_CENTRAL_DSCNT_PRC_LIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.750011+00:00'
    }
) }}

WITH 

source_wi_cpd_std_disc_vw AS (
    SELECT
        batch_id,
        disc_header_id,
        version_number,
        effective_start_date,
        effective_end_date,
        disc_type,
        code,
        name,
        status,
        sales_path,
        erp_pricelist_id,
        scenario,
        std_disc_type,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'wi_cpd_std_disc_vw') }}
),

final AS (
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
    FROM source_wi_cpd_std_disc_vw
)

SELECT * FROM final
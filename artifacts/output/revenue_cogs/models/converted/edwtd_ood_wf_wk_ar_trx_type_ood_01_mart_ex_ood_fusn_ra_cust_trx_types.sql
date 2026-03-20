{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_ar_trx_type_ood', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_WK_AR_TRX_TYPE_OOD',
        'target_table': 'EX_OOD_FUSN_RA_CUST_TRX_TYPES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.322602+00:00'
    }
) }}

WITH 

source_st_ood_fusn_ra_cust_trx_types AS (
    SELECT
        cust_trx_type_id,
        description,
        name,
        org_id,
        type_code,
        creation_date,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_ood_fusn_ra_cust_trx_types') }}
),

source_st_ood_ra_cust_trx_types AS (
    SELECT
        creation_date,
        cust_trx_type_id,
        description,
        name,
        org_id,
        type_code,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_ood_ra_cust_trx_types') }}
),

source_n_source_system_codes AS (
    SELECT
        source_system_code,
        source_system_name,
        database_name,
        company,
        edw_create_date,
        edw_create_user,
        edw_update_date,
        edw_update_user,
        global_name,
        gmt_offset
    FROM {{ source('raw', 'n_source_system_codes') }}
),

transformed_exp_w_om_ra_cust_trx_types AS (
    SELECT
    bk_company_code,
    set_of_books_key,
    bk_ar_trx_type_code,
    ar_trx_type_short_code,
    ar_trx_type_description,
    sk_cust_trx_type_id_int,
    ss_code,
    start_tv_date,
    end_tv_date,
    create_datetime,
    action_code,
    rank_index,
    dml_type,
    exception_type
    FROM source_n_source_system_codes
),

transformed_exp_ex_ood_ra_cust_trx_types AS (
    SELECT
    creation_date,
    cust_trx_type_id,
    description,
    name,
    org_id,
    type_code,
    last_update_date,
    create_datetime,
    action_code,
    exception_type
    FROM transformed_exp_w_om_ra_cust_trx_types
),

final AS (
    SELECT
        cust_trx_type_id,
        description,
        name,
        org_id,
        type_code,
        creation_date,
        last_update_date,
        create_datetime,
        action_code,
        exception_type
    FROM transformed_exp_ex_ood_ra_cust_trx_types
)

SELECT * FROM final
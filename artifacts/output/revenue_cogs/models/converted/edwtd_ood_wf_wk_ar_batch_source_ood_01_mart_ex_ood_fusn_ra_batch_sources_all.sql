{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_ar_batch_source_ood', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_WK_AR_BATCH_SOURCE_OOD',
        'target_table': 'EX_OOD_FUSN_RA_BATCH_SOURCES_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.839263+00:00'
    }
) }}

WITH 

source_ex_ood_fusn_ra_batch_sources_all AS (
    SELECT
        batch_source_id,
        batch_source_type,
        description,
        end_date,
        name,
        org_id,
        start_date,
        status,
        creation_date,
        last_update_date,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_ood_fusn_ra_batch_sources_all') }}
),

source_st_ood_ra_batch_sources_all AS (
    SELECT
        batch_source_id,
        batch_source_type,
        creation_date,
        description,
        end_date,
        name,
        org_id,
        start_date,
        status,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_ood_ra_batch_sources_all') }}
),

source_ev_op_unit_gaap_company AS (
    SELECT
        sk_organization_id_int,
        bk_company_code,
        set_of_books_key,
        bk_operating_unit_name_code
    FROM {{ source('raw', 'ev_op_unit_gaap_company') }}
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

transformed_exp_ex_ood_ra_batch_sources_all AS (
    SELECT
    batch_source_id,
    batch_source_type,
    creation_date,
    description,
    end_date,
    name,
    org_id,
    start_date,
    status,
    last_update_date,
    create_datetime,
    action_code,
    exception_type
    FROM source_n_source_system_codes
),

transformed_exp_om_ra_batch_sources_all AS (
    SELECT
    name,
    bk_operating_unit_name_code,
    batch_source_id,
    description,
    batch_source_type,
    start_tv_dt,
    start_date,
    end_date,
    status,
    source_system_code,
    ar_batch_source_key,
    org_id,
    end_tv_dt,
    rank_index,
    dml_type,
    action_code,
    exception_type,
    IFF(ISNULL(ACTION_CODE),'NN','NE') AS error_check,
    CURRENT_TIMESTAMP() AS update_datetime
    FROM transformed_exp_ex_ood_ra_batch_sources_all
),

final AS (
    SELECT
        batch_source_id,
        batch_source_type,
        description,
        end_date,
        name,
        org_id,
        start_date,
        status,
        creation_date,
        last_update_date,
        create_datetime,
        action_code,
        exception_type
    FROM transformed_exp_om_ra_batch_sources_all
)

SELECT * FROM final
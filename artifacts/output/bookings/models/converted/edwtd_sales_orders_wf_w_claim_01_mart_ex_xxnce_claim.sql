{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_claim', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_CLAIM',
        'target_table': 'EX_XXNCE_CLAIM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.887565+00:00'
    }
) }}

WITH 

source_st_xxnce_claim AS (
    SELECT
        claim_id,
        claim_status,
        territory_type_code,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        object_version_number,
        claim_reason_code,
        claim_comment,
        claim_value
    FROM {{ source('raw', 'st_xxnce_claim') }}
),

source_w_claim AS (
    SELECT
        claim_key,
        sk_claim_id,
        claim_comments_txt,
        claim_reason_txt,
        sales_terr_asgmt_type_cd,
        claim_status_name,
        create_dtm,
        last_update_dtm,
        claim_usd_amt,
        claim_reason_descr,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_claim') }}
),

transformed_exp_xxnce_claim AS (
    SELECT
    claim_id,
    claim_status,
    territory_type_code,
    creation_date,
    created_by,
    last_updated_date,
    last_updated_by,
    object_version_number,
    claim_reason_code,
    claim_comment,
    claim_value,
    trail_file_name,
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    'RI' AS exception_type
    FROM source_w_claim
),

final AS (
    SELECT
        claim_id,
        claim_status,
        territory_type_code,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        object_version_number,
        claim_reason_code,
        claim_comment,
        claim_value,
        exception_type
    FROM transformed_exp_xxnce_claim
)

SELECT * FROM final
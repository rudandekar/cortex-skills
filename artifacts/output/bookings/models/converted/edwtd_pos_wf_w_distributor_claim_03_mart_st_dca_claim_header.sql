{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_distributor_claim', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_DISTRIBUTOR_CLAIM',
        'target_table': 'ST_DCA_CLAIM_HEADER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.985021+00:00'
    }
) }}

WITH 

source_ex_dca_claim_header AS (
    SELECT
        claim_id,
        source_profile_id,
        promo_type,
        claim_type,
        claim_amt,
        currency_code,
        claim_header_status,
        claim_created_date,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        pos_trans_type,
        claim_business_status,
        cco_status,
        create_datetime,
        batch_id,
        action_code,
        exception_type,
        disti_provided_claim_ref_num,
        expired_claim,
        auto_validate
    FROM {{ source('raw', 'ex_dca_claim_header') }}
),

source_st_dca_claim_header AS (
    SELECT
        claim_id,
        source_profile_id,
        promo_type,
        claim_type,
        claim_amt,
        currency_code,
        claim_header_status,
        claim_created_date,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        pos_trans_type,
        claim_business_status,
        cco_status,
        create_datetime,
        batch_id,
        action_code,
        disti_provided_claim_ref_num,
        expired_claim,
        auto_validate
    FROM {{ source('raw', 'st_dca_claim_header') }}
),

final AS (
    SELECT
        claim_id,
        source_profile_id,
        promo_type,
        claim_type,
        claim_amt,
        currency_code,
        claim_header_status,
        claim_created_date,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        pos_trans_type,
        claim_business_status,
        cco_status,
        create_datetime,
        batch_id,
        action_code,
        disti_provided_claim_ref_num,
        expired_claim,
        auto_validate
    FROM source_st_dca_claim_header
)

SELECT * FROM final
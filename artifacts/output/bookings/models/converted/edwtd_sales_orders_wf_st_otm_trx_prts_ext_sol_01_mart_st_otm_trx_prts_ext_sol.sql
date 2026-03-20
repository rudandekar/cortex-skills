{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_trx_prts_ext_sol ', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_TRX_PRTS_EXT_SOL ',
        'target_table': 'ST_OTM_TRX_PRTS_EXT_SOL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.286188+00:00'
    }
) }}

WITH 

source_ff_otm_trx_prts_ext_sol AS (
    SELECT
        action_cd,
        assignment_code,
        assignment_source_system,
        batch_id,
        create_datetime,
        creation_date,
        enrichment_code,
        enrichment_party_type,
        expiration_date,
        global_name,
        hz_cr_party_id,
        last_update_date,
        order_line_id,
        reason_code,
        sav_hq_remote_flag,
        sav_id,
        sav_member_id,
        sav_member_party_id,
        sav_owned_flag,
        split_percent,
        start_date,
        status_code,
        trx_party_ext_id,
        user_name,
        validation_date,
        ucrm_case_num
    FROM {{ source('raw', 'ff_otm_trx_prts_ext_sol') }}
),

final AS (
    SELECT
        action_cd,
        assignment_code,
        assignment_source_system,
        batch_id,
        create_datetime,
        creation_date,
        enrichment_code,
        enrichment_party_type,
        expiration_date,
        global_name,
        hz_cr_party_id,
        last_update_date,
        order_line_id,
        reason_code,
        sav_hq_remote_flag,
        sav_id,
        sav_member_id,
        sav_member_party_id,
        sav_owned_flag,
        split_percent,
        start_date,
        status_code,
        trx_party_ext_id,
        user_name,
        validation_date,
        ucrm_case_num
    FROM source_ff_otm_trx_prts_ext_sol
)

SELECT * FROM final
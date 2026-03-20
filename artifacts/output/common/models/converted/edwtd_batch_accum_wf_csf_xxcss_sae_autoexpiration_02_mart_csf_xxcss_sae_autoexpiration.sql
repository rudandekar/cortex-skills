{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_xxcss_sae_autoexpiration', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCSS_SAE_AUTOEXPIRATION',
        'target_table': 'CSF_XXCSS_SAE_AUTOEXPIRATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.108951+00:00'
    }
) }}

WITH 

source_stg_xxcss_sae_autoexpiration AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        contract_number,
        saehold_begin_date,
        saehold_end_date,
        saehold_reason,
        last_updated_by,
        last_updated_date,
        saehold_requestor_email,
        saehold_status,
        cp_request_id,
        saehold_action,
        saeholdactual_end_date,
        saehold_account_name,
        saehold_cs_casenumber,
        saehold_approver1,
        saehold_approver2,
        saehold_approver3,
        saehold_netamount,
        chr_id
    FROM {{ source('raw', 'stg_xxcss_sae_autoexpiration') }}
),

source_csf_xxcss_sae_autoexpiration AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        contract_number,
        saehold_begin_date,
        saehold_end_date,
        saehold_reason,
        last_updated_by,
        last_updated_date,
        saehold_requestor_email,
        saehold_status,
        cp_request_id,
        saehold_action,
        saeholdactual_end_date,
        saehold_account_name,
        saehold_cs_casenumber,
        saehold_approver1,
        saehold_approver2,
        saehold_approver3,
        saehold_netamount,
        chr_id
    FROM {{ source('raw', 'csf_xxcss_sae_autoexpiration') }}
),

transformed_exptrans AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    contract_number,
    saehold_begin_date,
    saehold_end_date,
    saehold_reason,
    last_updated_by,
    last_updated_date,
    saehold_requestor_email,
    saehold_status,
    cp_request_id,
    saehold_action,
    saeholdactual_end_date,
    saehold_account_name,
    saehold_cs_casenumber,
    saehold_approver1,
    saehold_approver2,
    saehold_approver3,
    saehold_netamount,
    chr_id
    FROM source_csf_xxcss_sae_autoexpiration
),

final AS (
    SELECT
        source_dml_type,
        ges_update_date,
        refresh_datetime,
        contract_number,
        saehold_begin_date,
        saehold_end_date,
        saehold_reason,
        last_updated_by,
        last_updated_date,
        saehold_requestor_email,
        saehold_status,
        cp_request_id,
        saehold_action,
        saeholdactual_end_date,
        saehold_account_name,
        saehold_cs_casenumber,
        saehold_approver1,
        saehold_approver2,
        saehold_approver3,
        saehold_netamount,
        chr_id
    FROM transformed_exptrans
)

SELECT * FROM final
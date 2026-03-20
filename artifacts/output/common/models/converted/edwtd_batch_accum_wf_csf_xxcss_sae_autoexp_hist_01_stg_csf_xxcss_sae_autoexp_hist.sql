{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_xxcss_sae_autoexp_hist', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCSS_SAE_AUTOEXP_HIST',
        'target_table': 'STG_CSF_XXCSS_SAE_AUTOEXP_HIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.901864+00:00'
    }
) }}

WITH 

source_stg_csf_xxcss_sae_autoexp_hist AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        crequestid,
        contract_number,
        action,
        created_by,
        creation_date,
        message,
        saehold_begin_date,
        saehold_end_date,
        saehold_reason,
        saehold_requestor_email,
        saehold_status,
        saeholdactual_end_date,
        saehold_account_name,
        saehold_cs_casenumber,
        last_updated_by,
        last_updated_date,
        saehold_approver1,
        saehold_approver2,
        saehold_approver3,
        saehold_netamount,
        chr_id
    FROM {{ source('raw', 'stg_csf_xxcss_sae_autoexp_hist') }}
),

source_csf_xxcss_sae_autoexp_hist AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        crequestid,
        contract_number,
        action,
        created_by,
        creation_date,
        message,
        saehold_begin_date,
        saehold_end_date,
        saehold_reason,
        saehold_requestor_email,
        saehold_status,
        saeholdactual_end_date,
        saehold_account_name,
        saehold_cs_casenumber,
        last_updated_by,
        last_updated_date,
        saehold_approver1,
        saehold_approver2,
        saehold_approver3,
        saehold_netamount,
        chr_id
    FROM {{ source('raw', 'csf_xxcss_sae_autoexp_hist') }}
),

transformed_exptrans AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    crequestid,
    contract_number,
    action,
    created_by,
    creation_date,
    message,
    saehold_begin_date,
    saehold_end_date,
    saehold_reason,
    saehold_requestor_email,
    saehold_status,
    saeholdactual_end_date,
    saehold_account_name,
    saehold_cs_casenumber,
    last_updated_by,
    last_updated_date,
    saehold_approver1,
    saehold_approver2,
    saehold_approver3,
    saehold_netamount,
    chr_id
    FROM source_csf_xxcss_sae_autoexp_hist
),

final AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        crequestid,
        contract_number,
        action,
        created_by,
        creation_date,
        message,
        saehold_begin_date,
        saehold_end_date,
        saehold_reason,
        saehold_requestor_email,
        saehold_status,
        saeholdactual_end_date,
        saehold_account_name,
        saehold_cs_casenumber,
        last_updated_by,
        last_updated_date,
        saehold_approver1,
        saehold_approver2,
        saehold_approver3,
        saehold_netamount,
        chr_id
    FROM transformed_exptrans
)

SELECT * FROM final
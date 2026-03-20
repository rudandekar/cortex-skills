{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_okc_k_items', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_OKC_K_ITEMS',
        'target_table': 'CSF_OKC_K_ITEMS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.861907+00:00'
    }
) }}

WITH 

source_stg_okc_k_items AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        id,
        cle_id,
        chr_id,
        cle_id_for,
        dnz_chr_id,
        object1_id1,
        object1_id2,
        jtot_object1_code,
        uom_code,
        exception_yn,
        number_of_items,
        priced_item_yn,
        object_version_number,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        security_group_id,
        upg_orig_system_ref,
        upg_orig_system_ref_id,
        program_application_id,
        program_id,
        program_update_date,
        request_id
    FROM {{ source('raw', 'stg_okc_k_items') }}
),

source_csf_okc_k_items AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        id,
        cle_id,
        chr_id,
        cle_id_for,
        dnz_chr_id,
        object1_id1,
        object1_id2,
        jtot_object1_code,
        uom_code,
        exception_yn,
        number_of_items,
        priced_item_yn,
        object_version_number,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        security_group_id,
        upg_orig_system_ref,
        upg_orig_system_ref_id,
        program_application_id,
        program_id,
        program_update_date,
        request_id
    FROM {{ source('raw', 'csf_okc_k_items') }}
),

transformed_exptrans AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    id,
    cle_id,
    chr_id,
    cle_id_for,
    dnz_chr_id,
    object1_id1,
    object1_id2,
    jtot_object1_code,
    uom_code,
    exception_yn,
    number_of_items,
    priced_item_yn,
    object_version_number,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    security_group_id,
    upg_orig_system_ref,
    upg_orig_system_ref_id,
    program_application_id,
    program_id,
    program_update_date,
    request_id
    FROM source_csf_okc_k_items
),

final AS (
    SELECT
        source_dml_type,
        ges_update_date,
        refresh_datetime,
        id,
        cle_id,
        chr_id,
        cle_id_for,
        dnz_chr_id,
        object1_id1,
        object1_id2,
        jtot_object1_code,
        uom_code,
        exception_yn,
        number_of_items,
        priced_item_yn,
        object_version_number,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        security_group_id,
        upg_orig_system_ref,
        upg_orig_system_ref_id,
        program_application_id,
        program_id,
        program_update_date,
        request_id
    FROM transformed_exptrans
)

SELECT * FROM final
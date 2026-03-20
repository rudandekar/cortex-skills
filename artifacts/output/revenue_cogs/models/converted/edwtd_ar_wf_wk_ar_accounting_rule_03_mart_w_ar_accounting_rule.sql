{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_ar_accounting_rule', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WK_AR_ACCOUNTING_RULE',
        'target_table': 'W_AR_ACCOUNTING_RULE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.687448+00:00'
    }
) }}

WITH 

source_st_cg_ra_rules AS (
    SELECT
        batch_id,
        rule_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        name,
        type_code,
        status,
        frequency,
        occurrences,
        description,
        attribute_category,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        deferred_revenue_flag,
        source_commit_time,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg_ra_rules') }}
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

source_st_om_ra_rules AS (
    SELECT
        batch_id,
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute_category,
        created_by,
        creation_date,
        deferred_revenue_flag,
        description,
        frequency,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        name,
        occurrences,
        rule_id,
        status,
        type_code,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_ra_rules') }}
),

transformed_ex_st_om_ra_rules AS (
    SELECT
    batch_id,
    attribute1,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    name,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute_category,
    created_by,
    creation_date,
    deferred_revenue_flag,
    description,
    rule_id,
    frequency,
    ges_update_date,
    global_name,
    last_updated_by,
    last_update_date,
    last_update_login,
    occurrences,
    status,
    type_code,
    create_datetime,
    action_code,
    'RI' AS exception_type
    FROM source_st_om_ra_rules
),

transformed_exp_w_ar_accounting_rule_cg AS (
    SELECT
    bk_accounting_rule_name,
    ar_accounting_rule_description,
    sk_rule_id_int,
    ss_code,
    start_date_active,
    end_date_active,
    batch_id,
    action_code,
    exception_type,
    rank_index,
    dml_type,
    create_datetime
    FROM transformed_ex_st_om_ra_rules
),

transformed_exp_w_ar_accounting_rule_cdc AS (
    SELECT
    bk_accounting_rule_name,
    ar_accounting_rule_description,
    sk_rule_id_int,
    ss_code,
    start_date_active,
    end_date_active,
    batch_id,
    action_code,
    exception_type,
    rank_index,
    dml_type,
    create_datetime
    FROM transformed_exp_w_ar_accounting_rule_cg
),

final AS (
    SELECT
        bk_accounting_rule_name,
        start_tv_date,
        end_tv_date,
        ar_accounting_rule_description,
        sk_rule_id_int,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code,
        dml_type
    FROM transformed_exp_w_ar_accounting_rule_cdc
)

SELECT * FROM final
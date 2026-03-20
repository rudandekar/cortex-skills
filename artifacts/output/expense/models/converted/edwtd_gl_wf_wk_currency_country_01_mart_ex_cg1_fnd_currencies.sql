{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_currency_country', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_CURRENCY_COUNTRY',
        'target_table': 'EX_CG1_FND_CURRENCIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.976867+00:00'
    }
) }}

WITH 

source_st_cg1_fnd_currencies AS (
    SELECT
        batch_id,
        currency_code,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        enabled_flag,
        currency_flag,
        iso_flag,
        description,
        issuing_territory_code,
        precision_r,
        extended_precision,
        symbol,
        start_date_active,
        end_date_active,
        minimum_accountable_unit,
        context,
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
        global_attribute_category,
        global_attribute1,
        global_attribute2,
        global_attribute3,
        global_attribute4,
        global_attribute5,
        global_attribute6,
        global_attribute7,
        global_attribute8,
        global_attribute9,
        global_attribute10,
        global_attribute11,
        global_attribute12,
        global_attribute13,
        global_attribute14,
        global_attribute15,
        global_attribute16,
        global_attribute17,
        global_attribute18,
        global_attribute19,
        global_attribute20,
        derive_effective,
        derive_type,
        derive_factor,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg1_fnd_currencies') }}
),

source_n_iso_country_tv AS (
    SELECT
        bk_iso_country_code,
        start_tv_date,
        end_tv_date,
        iso_country_name,
        sk_entity_name,
        edw_create_datetime,
        edw_update_datetime,
        edw_update_user,
        edw_create_user
    FROM {{ source('raw', 'n_iso_country_tv') }}
),

source_st_mf_fnd_currencies AS (
    SELECT
        batch_id,
        currency_code,
        global_name,
        issuing_territory_code,
        enabled_flag,
        iso_flag,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_fnd_currencies') }}
),

transformed_exp_w_currency_country AS (
    SELECT
    batch_id,
    issuing_territory_code,
    currency_code,
    start_tv_date,
    end_tv_date,
    create_datetime,
    action_code,
    dml_type,
    exception_type,
    rank_index
    FROM source_st_mf_fnd_currencies
),

final AS (
    SELECT
        batch_id,
        currency_code,
        issuing_territory_code,
        global_name,
        create_datetime,
        action_code,
        exception_type
    FROM transformed_exp_w_currency_country
)

SELECT * FROM final
{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_mf_fnd_currencies', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_MF_FND_CURRENCIES',
        'target_table': 'FF_MF_FND_CURRENCIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.650546+00:00'
    }
) }}

WITH 

source_mf_fnd_currencies AS (
    SELECT
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
        context,
        created_by,
        creation_date,
        currency_code,
        currency_flag,
        derive_effective,
        derive_factor,
        derive_type,
        description,
        enabled_flag,
        end_date_active,
        extended_precision,
        ges_update_date,
        global_attribute1,
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
        global_attribute2,
        global_attribute20,
        global_attribute3,
        global_attribute4,
        global_attribute5,
        global_attribute6,
        global_attribute7,
        global_attribute8,
        global_attribute9,
        global_attribute_category,
        global_name,
        iso_flag,
        issuing_territory_code,
        last_updated_by,
        last_update_date,
        last_update_login,
        minimum_accountable_unit,
        precision,
        start_date_active,
        symbol
    FROM {{ source('raw', 'mf_fnd_currencies') }}
),

transformed_exp_set_default_values AS (
    SELECT
    currency_code,
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_create_datetime,
    'I' AS o_action_code
    FROM source_mf_fnd_currencies
),

final AS (
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
    FROM transformed_exp_set_default_values
)

SELECT * FROM final
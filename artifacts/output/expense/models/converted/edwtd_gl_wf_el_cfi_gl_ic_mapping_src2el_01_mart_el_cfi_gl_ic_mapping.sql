{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cfi_gl_ic_mapping_src2el', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_CFI_GL_IC_MAPPING_SRC2EL',
        'target_table': 'EL_CFI_GL_IC_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.107770+00:00'
    }
) }}

WITH 

source_mf_cfi_gl_ic_mapping AS (
    SELECT
        address1,
        address2,
        address3,
        attribute1,
        attribute10,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        country_code,
        country_name,
        created_by,
        creation_date,
        currency_code,
        database_link,
        database_name,
        email,
        euro_currency,
        ges_update_date,
        global_name,
        ic_account,
        last_updated_by,
        last_update_date,
        set_of_books_id,
        source_sob_id,
        structure_size,
        vat_number
    FROM {{ source('raw', 'mf_cfi_gl_ic_mapping') }}
),

update_strategy_ins_updtrans AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM source_mf_cfi_gl_ic_mapping
    WHERE DD_INSERT != 3
),

update_strategy_upd_updtrans AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_ins_updtrans
    WHERE DD_UPDATE != 3
),

lookup_lkptrans AS (
    SELECT
        a.*,
        b.*
    FROM update_strategy_upd_updtrans a
    LEFT JOIN {{ source('raw', 'el_cfi_gl_ic_mapping') }} b
        ON a.in_country_code = b.in_country_code
),

transformed_exptrans AS (
    SELECT
    address1,
    address2,
    address3,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    country_code,
    country_name,
    currency_code,
    email,
    euro_currency,
    ges_update_date,
    global_name,
    ic_account,
    set_of_books_id,
    source_sob_id,
    vat_number,
    RTRIM(LTRIM(TO_CHAR(COUNTRY_CODE))) AS out_country_code,
    RTRIM(LTRIM(TO_CHAR(CURRENCY_CODE))) AS out_currency_code,
    RTRIM(LTRIM(TO_CHAR(EURO_CURRENCY))) AS out_euro_currency,
    RTRIM(LTRIM(TO_CHAR(GLOBAL_NAME))) AS out_global_name,
    TO_INTEGER(SET_OF_BOOKS_ID) AS out_set_of_books_id,
    TO_INTEGER(SOURCE_SOB_ID) AS out_source_sob_id
    FROM lookup_lkptrans
),

transformed_exptrans1 AS (
    SELECT
    address1,
    address2,
    address3,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    country_code,
    country_name,
    currency_code,
    email,
    euro_currency,
    ges_update_date,
    global_name,
    ic_account,
    set_of_books_id,
    source_sob_id,
    vat_number,
    lkp_address1,
    lkp_address2,
    lkp_address3,
    lkp_attribute1,
    lkp_attribute2,
    lkp_attribute3,
    lkp_attribute4,
    lkp_country_code,
    lkp_country_name,
    lkp_currency_code,
    lkp_email,
    lkp_euro_currency,
    lkp_ges_update_date,
    lkp_global_name,
    lkp_ic_account,
    lkp_set_of_books_id,
    lkp_source_sob_id,
    lkp_vat_number
    FROM transformed_exptrans
),

routed_rtrtrans AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM transformed_exptrans1
),

final AS (
    SELECT
        address1,
        address2,
        address3,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        country_code,
        country_name,
        currency_code,
        email,
        euro_currency,
        ges_update_date,
        global_name,
        ic_account,
        set_of_books_id,
        source_sob_id,
        vat_number
    FROM routed_rtrtrans
)

SELECT * FROM final
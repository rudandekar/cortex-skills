{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_vendor_inv_dstrbn_trx_type_src23nf', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_VENDOR_INV_DSTRBN_TRX_TYPE_SRC23NF',
        'target_table': 'N_VENDOR_INV_DSTRBN_TRX_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.840057+00:00'
    }
) }}

WITH 

source_cg1_fnd_lookup_values AS (
    SELECT
        lookup_type,
        language,
        lookup_code,
        meaning,
        enabled_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        source_lang,
        security_group_id,
        view_application_id,
        description,
        start_date_active,
        end_date_active,
        last_update_login,
        territory_code,
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
        tag,
        leaf_node,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'cg1_fnd_lookup_values') }}
),

routed_rtr_n_vendor_inv_dstrbn_trx_type AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM source_cg1_fnd_lookup_values
),

update_strategy_upd_n_vendor_inv_dstrbn_trx_type_ins AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM routed_rtr_n_vendor_inv_dstrbn_trx_type
    WHERE DD_INSERT != 3
),

transformed_exp_n_vendor_inv_dstrbn_trx_type AS (
    SELECT
    bk_vndr_inv_dstrbn_trx_type_cd,
    lookup_code,
    meaning,
    description,
    CURRENT_TIMESTAMP() AS edw_update_dtm
    FROM update_strategy_upd_n_vendor_inv_dstrbn_trx_type_ins
),

transformed_exp_ap_lookup_codes AS (
    SELECT
    lookup_type,
    lookup_code,
    meaning,
    description,
    enabled_flag,
    start_date_active,
    end_date_active,
    LTRIM(RTRIM(LOOKUP_CODE)) AS o_lookup_code
    FROM transformed_exp_n_vendor_inv_dstrbn_trx_type
),

update_strategy_upd_n_vendor_inv_dstrbn_trx_type_upd AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_ap_lookup_codes
    WHERE DD_UPDATE != 3
),

lookup_lkp_n_vendor_inv_dstrbn_trx_type AS (
    SELECT
        a.*,
        b.*
    FROM update_strategy_upd_n_vendor_inv_dstrbn_trx_type_upd a
    LEFT JOIN {{ source('raw', 'n_vendor_inv_dstrbn_trx_type') }} b
        ON a.lookup_code = b.lookup_code
),

final AS (
    SELECT
        bk_vndr_inv_dstrbn_trx_type_cd,
        vndr_inv_dstrbn_trx_type_name,
        vndr_inv_dstrbn_trx_type_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM lookup_lkp_n_vendor_inv_dstrbn_trx_type
)

SELECT * FROM final
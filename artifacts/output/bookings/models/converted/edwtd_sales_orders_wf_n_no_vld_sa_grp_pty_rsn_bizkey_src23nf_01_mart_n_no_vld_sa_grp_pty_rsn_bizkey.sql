{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_no_vld_sa_grp_pty_rsn_bizkey_src23nf', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_NO_VLD_SA_GRP_PTY_RSN_BIZKEY_SRC23NF',
        'target_table': 'N_NO_VLD_SA_GRP_PTY_RSN_BIZKEY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.604572+00:00'
    }
) }}

WITH 

source_fnd_lookup_values AS (
    SELECT
        lookup_type,
        language,
        lookup_code,
        meaning,
        description,
        enabled_flag,
        start_date_active,
        end_date_active,
        created_by,
        creation_date,
        last_updated_by,
        last_update_login,
        last_update_date,
        source_lang,
        security_group_id,
        view_application_id,
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
        leaf_node
    FROM {{ source('raw', 'fnd_lookup_values') }}
),

lookup_lkptrans AS (
    SELECT
        a.*,
        b.*
    FROM source_fnd_lookup_values a
    LEFT JOIN {{ source('raw', 'n_no_vld_sa_grp_pty_rsn_bizkey') }} b
        ON a.lookup_code = b.lookup_code
),

transformed_exptrans AS (
    SELECT
    lkp_no_vld_sls_acct_pty_rsn_name,
    lookup_code,
    meaning,
    start_date_active,
    end_date_active,
    'MASTER' AS ep_source_cd,
    CURRENT_DATE() AS edw_create_dtm,
    'E_SLSORD_BATCH' AS edw_create_user,
    CURRENT_DATE() AS edw_update_dtm,
    'E_SLSORD_BATCH' AS edw_update_user
    FROM lookup_lkptrans
),

routed_rtrtrans AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'UPDATE'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'INSERT'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM transformed_exptrans
),

update_strategy_upd_n_no_vld_sa_grp_pty_rsn_bizkey AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM routed_rtrtrans
    WHERE DD_UPDATE != 3
),

update_strategy_insert_n_no_vld_sa_grp_pty_rsn_bizkey AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_upd_n_no_vld_sa_grp_pty_rsn_bizkey
    WHERE DD_INSERT != 3
),

final AS (
    SELECT
        no_vld_sls_acct_pty_rsn_name,
        ep_source_cd,
        no_vld_sls_acct_pty_rsn_descr,
        reason_start_dt,
        reason_end_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_insert_n_no_vld_sa_grp_pty_rsn_bizkey
)

SELECT * FROM final
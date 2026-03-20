{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_credit_type_tv_src23nf', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_CREDIT_TYPE_TV_SRC23NF',
        'target_table': 'N_SALES_CREDIT_TYPE_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.036296+00:00'
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

transformed_exp_update AS (
    SELECT
    bk_sales_credit_type_code,
    last_update_date,
    edw_update_dtm,
    start_tv_date2,
    ADD_TO_DATE(LAST_UPDATE_DATE,'dd',-1) AS end_tv_date
    FROM source_fnd_lookup_values
),

update_strategy_upd_update_n_sales_credit_type_tv AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_update
    WHERE DD_UPDATE != 3
),

transformed_exp_n_sales_credit_type_tv AS (
    SELECT
    lkp_bk_sales_credit_type_code,
    lkp_sales_credit_type_description,
    lkp__sales_credit_type_enabled_flag,
    lkp_sales_credit_type_quota_flag,
    bk_sales_credit_type_code,
    end_tv_date,
    sales_credit_type_description,
    sales_credit_type_enabled_flag,
    sales_credit_type_quota_flag,
    last_update_date,
    start_tv_date,
    CURRENT_DATE() AS edw_create_dtm,
    'E_SLSORD_BATCH' AS edw_create_user,
    CURRENT_DATE() AS edw_update_dtm,
    'E_SLSORD_BATCH' AS edw_update_user,
    IFF(ISNULL(LKP_BK_SALES_CREDIT_TYPE_CODE),1,IFF(LKP_SALES_CREDIT_TYPE_DESCRIPTION!=SALES_CREDIT_TYPE_DESCRIPTION OR LKP__SALES_CREDIT_TYPE_ENABLED_FLAG!=SALES_CREDIT_TYPE_ENABLED_FLAG OR LKP_SALES_CREDIT_TYPE_QUOTA_FLAG!=SALES_CREDIT_TYPE_QUOTA_FLAG,2,3)) AS flag
    FROM update_strategy_upd_update_n_sales_credit_type_tv
),

routed_rtr_n_sales_credit_type_tv AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM transformed_exp_n_sales_credit_type_tv
),

lookup_lkp_n_sales_credit_type_tv AS (
    SELECT
        a.*,
        b.*
    FROM routed_rtr_n_sales_credit_type_tv a
    LEFT JOIN {{ source('raw', 'n_sales_credit_type_tv') }} b
        ON a.in_bk_sales_credit_type_code = b.in_bk_sales_credit_type_code
),

update_strategy_upd_insert_n_sales_credit_type_tv AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM lookup_lkp_n_sales_credit_type_tv
    WHERE DD_INSERT != 3
),

final AS (
    SELECT
        bk_sales_credit_type_code,
        start_tv_date,
        end_tv_date,
        sales_credit_type_description,
        sales_credit_type_enabled_flag,
        sales_credit_type_quota_flag,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM update_strategy_upd_insert_n_sales_credit_type_tv
)

SELECT * FROM final
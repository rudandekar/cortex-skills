{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_erp_freight_terms', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_ERP_FREIGHT_TERMS',
        'target_table': 'W_ERP_FREIGHT_TERMS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.547806+00:00'
    }
) }}

WITH 

source_el_fnd_lookup_values AS (
    SELECT
        lookup_type,
        lang,
        security_group_id,
        lookup_code,
        meaning,
        view_application_id,
        global_name,
        enabled_flag,
        description,
        create_datetime,
        ges_update_date,
        start_date_active,
        end_date_active
    FROM {{ source('raw', 'el_fnd_lookup_values') }}
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

transformed_exp_el_fnd_lookup_values AS (
    SELECT
    lookup_code,
    ges_update_date,
    end_tv_date,
    meaning,
    description,
    enabled_flag,
    start_date_active,
    end_date_active,
    source_system_code,
    action_code,
    edw_update_datetime,
    rank_index,
    dml_type
    FROM source_n_source_system_codes
),

final AS (
    SELECT
        bk_freight_terms_code,
        start_tv_date,
        end_tv_date,
        freight_terms_name,
        freight_terms_description,
        freight_terms_enabled_flag,
        freight_terms_start_active_dtm,
        freight_terms_end_active_dtm,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        edw_observation_datetime,
        action_code,
        dml_type
    FROM transformed_exp_el_fnd_lookup_values
)

SELECT * FROM final
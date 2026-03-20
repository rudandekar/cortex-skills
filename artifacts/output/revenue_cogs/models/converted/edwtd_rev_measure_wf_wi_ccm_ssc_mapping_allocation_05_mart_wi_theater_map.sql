{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ccm_ssc_mapping_allocation', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CCM_SSC_MAPPING_ALLOCATION',
        'target_table': 'WI_THEATER_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.160806+00:00'
    }
) }}

WITH 

source_wi_duty_vat_by_country AS (
    SELECT
        country_code,
        theater,
        country,
        vat_recovable,
        effective_duty,
        effective_vat,
        civ_rate,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'wi_duty_vat_by_country') }}
),

source_twfm065_freight_wh_multiplier AS (
    SELECT
        country_code,
        service_request,
        theater,
        country,
        multiplier
    FROM {{ source('raw', 'twfm065_freight_wh_multiplier') }}
),

source_twfm007_theater_map AS (
    SELECT
        ext_theater_name,
        theater_name,
        created_id,
        created_date,
        updated_id,
        updated_date,
        dept
    FROM {{ source('raw', 'twfm007_theater_map') }}
),

source_ccm_tac_mapping_allocation AS (
    SELECT
        fiscal_month_id,
        pl_node_value,
        mapping,
        delivery_theater,
        delivery_channel,
        cost_type,
        allocation_method,
        oh_category
    FROM {{ source('raw', 'ccm_tac_mapping_allocation') }}
),

source_ccm_ssc_mapping_allocation AS (
    SELECT
        fiscal_month_id,
        pl_node_value,
        account_number,
        sub_account_number,
        functional_cost,
        cost_type,
        allocation_method,
        theater
    FROM {{ source('raw', 'ccm_ssc_mapping_allocation') }}
),

source_wi_tss_cost_cntrl_month_id AS (
    SELECT
        fiscal_month_id,
        fiscal_year_month_int,
        fiscal_quarter_name,
        fiscal_month_name,
        month_rank
    FROM {{ source('raw', 'wi_tss_cost_cntrl_month_id') }}
),

transformed_exptrans AS (
    SELECT
    in_fiscal_year_month_int,
    pl_node_value,
    mapping,
    delivery_theater,
    delivery_channel,
    cost_type,
    allocation_method,
    oh_category
    FROM source_wi_tss_cost_cntrl_month_id
),

joined_jnrtrans AS (
    SELECT a.*, b.*
    FROM transformed_exptrans a
    INNER JOIN {{ source('raw', 'twfm065_freight_wh_multiplier') }} b
        ON FISCAL_MONTH_ID1 = FISCAL_MONTH_ID
),

final AS (
    SELECT
        ext_theater_name,
        theater_name,
        created_id,
        created_date,
        updated_id,
        updated_date,
        dept
    FROM joined_jnrtrans
)

SELECT * FROM final
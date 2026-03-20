{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_return_to_vendor', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_RETURN_TO_VENDOR',
        'target_table': 'N_RETURN_TO_VENDOR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.572103+00:00'
    }
) }}

WITH 

source_w_return_to_vendor AS (
    SELECT
        bk_shipset_num_int,
        sales_order_key,
        bk_carton_id,
        return_route_cd,
        return_reason_cd,
        dv_creation_dt,
        creation_dtm,
        rtv_num_int,
        return_to_vendor_status_cd,
        with_slc_role,
        ru_in_transit_role,
        ru_strtgc_logistics_cntr_name,
        ru_return_to_cm_from_slc_dtm,
        ru_dv_return_to_cm_from_slc_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_return_to_vendor') }}
),

final AS (
    SELECT
        bk_shipset_num_int,
        sales_order_key,
        bk_carton_id,
        return_route_cd,
        return_reason_cd,
        dv_creation_dt,
        creation_dtm,
        rtv_num_int,
        return_to_vendor_status_cd,
        with_slc_role,
        ru_in_transit_role,
        ru_strtgc_logistics_cntr_name,
        ru_return_to_cm_from_slc_dtm,
        ru_dv_return_to_cm_from_slc_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_return_to_vendor
)

SELECT * FROM final
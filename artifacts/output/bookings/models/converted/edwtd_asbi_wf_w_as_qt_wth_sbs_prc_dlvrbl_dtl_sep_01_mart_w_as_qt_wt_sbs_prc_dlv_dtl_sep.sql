{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_as_qt_wth_sbs_prc_dlvrbl_dtl_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_W_AS_QT_WTH_SBS_PRC_DLVRBL_DTL_SEP',
        'target_table': 'W_AS_QT_WT_SBS_PRC_DLV_DTL_SEP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.283083+00:00'
    }
) }}

WITH 

source_w_as_qt_wt_sbs_prc_dlv_dtl_sep AS (
    SELECT
        bk_deliverable_name,
        bk_service_component_name,
        bk_service_program_name,
        bk_as_quote_num,
        total_trips_cnt,
        total_nonlabor_usd_cost,
        total_work_hrs_qty,
        total_nights_away_cnt,
        service_prd_net_price_usd_amt,
        service_prd_lst_price_usd_amt,
        service_prd_cost_usd_amt,
        service_prd_qty,
        ordered_qty,
        deliverable_type_cd,
        source_deleted_flg,
        total_travel_hrs_cnt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_cd,
        onsite_hrs_cnt,
        remote_hrs_cnt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_as_qt_wt_sbs_prc_dlv_dtl_sep') }}
),

final AS (
    SELECT
        bk_deliverable_name,
        bk_service_component_name,
        bk_service_program_name,
        bk_as_quote_num,
        total_trips_cnt,
        total_nonlabor_usd_cost,
        total_work_hrs_qty,
        total_nights_away_cnt,
        service_prd_net_price_usd_amt,
        service_prd_lst_price_usd_amt,
        service_prd_cost_usd_amt,
        service_prd_qty,
        ordered_qty,
        deliverable_type_cd,
        source_deleted_flg,
        total_travel_hrs_cnt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_cd,
        onsite_hrs_cnt,
        remote_hrs_cnt,
        action_code,
        dml_type
    FROM source_w_as_qt_wt_sbs_prc_dlv_dtl_sep
)

SELECT * FROM final
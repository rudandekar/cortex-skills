{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_as_qt_wth_sbs_prc_dlvrbl_dtl_hdp', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_ST_AS_QT_WTH_SBS_PRC_DLVRBL_DTL_HDP',
        'target_table': 'ST_AS_QT_WTH_SBS_PRC_DLVRBL_DTL_HDP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.674849+00:00'
    }
) }}

WITH 

source_ff_as_qt_wth_sbs_prc_dlvrbl_dtl_hdp AS (
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
        ss_cd,
        onsite_hrs_cnt,
        remote_hrs_cnt
    FROM {{ source('raw', 'ff_as_qt_wth_sbs_prc_dlvrbl_dtl_hdp') }}
),

transformed_exptrans1 AS (
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
    ss_cd,
    onsite_hrs_cnt,
    remote_hrs_cnt,
    CURRENT_TIMESTAMP() AS created_dtm
    FROM source_ff_as_qt_wth_sbs_prc_dlvrbl_dtl_hdp
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
        ss_cd,
        created_dtm,
        onsite_hrs_cnt,
        remote_hrs_cnt
    FROM transformed_exptrans1
)

SELECT * FROM final
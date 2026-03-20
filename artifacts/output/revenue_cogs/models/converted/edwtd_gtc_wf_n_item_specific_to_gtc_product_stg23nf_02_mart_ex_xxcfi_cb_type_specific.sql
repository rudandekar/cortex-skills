{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_item_specific_to_gtc_product_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_ITEM_SPECIFIC_TO_GTC_PRODUCT_STG23NF',
        'target_table': 'EX_XXCFI_CB_TYPE_SPECIFIC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.409573+00:00'
    }
) }}

WITH 

source_n_item_specific_to_gtc_product AS (
    SELECT
        bk_gtc_product_type_cd,
        bk_ss_cd,
        deactivated_role,
        ru_deactivated_dt,
        ru_dactvtd_cisco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        customs_item_specific_key
    FROM {{ source('raw', 'n_item_specific_to_gtc_product') }}
),

source_ex_xxcfi_cb_type_specific AS (
    SELECT
        batch_id,
        classification_attributes_id,
        product_type_class_code,
        product_flag,
        description,
        specific1,
        comments,
        attribute1,
        attribute2,
        attribute3,
        deactivated_by,
        deactivated_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        source1,
        create_datetime,
        action_code,
        exception_type,
        specific_id
    FROM {{ source('raw', 'ex_xxcfi_cb_type_specific') }}
),

final AS (
    SELECT
        batch_id,
        classification_attributes_id,
        product_type_class_code,
        product_flag,
        description,
        specific1,
        comments,
        attribute1,
        attribute2,
        attribute3,
        deactivated_by,
        deactivated_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        source1,
        create_datetime,
        action_code,
        exception_type,
        specific_id
    FROM source_ex_xxcfi_cb_type_specific
)

SELECT * FROM final
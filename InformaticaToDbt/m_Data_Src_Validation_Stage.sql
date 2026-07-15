=============================================
Author: Ascendion AAVA
Created on:
Description: Informatica to dbt Conversion - Data Source Validation Stage Mapping
=============================================
-- =========================================================
-- CONVERSION LOG
-- =========================================================
-- Input Type : Informatica Mapping (m_Data_Src_Validation_Stage)
-- Target Platform : dbt (SQL Model)
-- Conversion Approach :
-- - Translated Informatica mapping logic (transformations, expressions, lookups) into dbt SQL model constructs
-- - Replaced Informatica built-in functions (IIF, DECODE, NVL, TRUNC, INSTR, SUBSTR, SYSDATE, etc.) with dbt/Jinja + warehouse SQL equivalents
-- - Converted Informatica sources and targets into dbt {{ source() }} / {{ ref() }} references
-- - Replaced Update Strategy transformations with dbt incremental model logic (is_incremental(), unique_key)
-- - Converted Rank / Sequence transformations into window functions or surrogate key macros (e.g. dbt_utils.generate_surrogate_key)
-- - Converted Slowly Changing Dimension logic into dbt snapshot constructs where applicable
-- - Added inline migration comments where source logic required dbt-specific restructuring
-- - Ensured compatibility with dbt Core / dbt Cloud on the target warehouse
-- Major Risks / Checks :
-- - Validate datatype mappings between Informatica source/target ports and warehouse column types
-- - Validate NULL semantics and string concatenation behavior
-- - Validate incremental/merge logic against the original Update Strategy behavior
-- - Validate that the ref()/source() dependency graph resolves correctly (no missing or broken sources)
-- - Validate materialization choice (view / table / incremental) matches the original load pattern
-- - Validate companion schema.yml tests reflect the original Informatica data quality checks
-- =========================================================

{{ config(
    materialized='incremental',
    unique_key='DATA_SRC_VAL_STAGE_ID',
    tags=['data_validation', 'staging']
) }}

-- =========================================================
-- SOURCE CTE: sq_DATA_SRC_VAL
-- Reason: Converted Informatica Source Qualifier to dbt source reference
-- =========================================================
with sq_DATA_SRC_VAL as (
    select
        DATA_SRC_VAL_ID,
        DATA_SRC_ID,
        VAL_RULE_ID,
        VAL_EXEC_ID,
        VAL_EXEC_STAT_CD,
        VAL_EXEC_START_TS,
        VAL_EXEC_END_TS,
        VAL_EXEC_ELAPSE_SEC,
        VAL_EXEC_RESULT_TXT,
        VAL_EXEC_RESULT_CNT,
        VAL_EXEC_RESULT_THRSHLD_CNT,
        LAST_UPDT_TS,
        LAST_UPDT_USER_ID
    from {{ source('data_validation', 'DATA_SRC_VAL') }}
    {% if is_incremental() %}
    -- Reason: Incremental load filter to process only new or updated records
    where LAST_UPDT_TS > (select max(LAST_UPDT_TS) from {{ this }})
    {% endif %}
),

-- =========================================================
-- EXPRESSION TRANSFORMATION: exp_DATA_SRC_VAL_STAGE
-- Reason: Converted Informatica Expression transformation to SQL CASE expressions
-- =========================================================
exp_DATA_SRC_VAL_STAGE as (
    select
        DATA_SRC_VAL_ID,
        DATA_SRC_ID,
        VAL_RULE_ID,
        VAL_EXEC_ID,
        VAL_EXEC_STAT_CD,
        VAL_EXEC_START_TS,
        VAL_EXEC_END_TS,
        VAL_EXEC_ELAPSE_SEC,
        VAL_EXEC_RESULT_TXT,
        VAL_EXEC_RESULT_CNT,
        VAL_EXEC_RESULT_THRSHLD_CNT,
        LAST_UPDT_TS,
        LAST_UPDT_USER_ID,
        -- Reason: Replaced Informatica SYSDATE with CURRENT_TIMESTAMP for dbt compatibility
        CURRENT_TIMESTAMP as CURR_TS,
        -- Reason: Replaced Informatica SYSDATE with CURRENT_DATE for dbt compatibility
        CURRENT_DATE as CURR_DT,
        -- Reason: Generated surrogate key using dbt_utils macro instead of Informatica sequence
        {{ dbt_utils.generate_surrogate_key(['DATA_SRC_VAL_ID', 'DATA_SRC_ID', 'VAL_RULE_ID']) }} as DATA_SRC_VAL_STAGE_ID
    from sq_DATA_SRC_VAL
),

-- =========================================================
-- LOOKUP TRANSFORMATION: lkp_DATA_SRC_VAL_STAGE
-- Reason: Converted Informatica Lookup to LEFT JOIN to check for existing records
-- =========================================================
lkp_DATA_SRC_VAL_STAGE as (
    select
        DATA_SRC_VAL_STAGE_ID,
        DATA_SRC_VAL_ID,
        DATA_SRC_ID,
        VAL_RULE_ID,
        VAL_EXEC_ID,
        VAL_EXEC_STAT_CD,
        VAL_EXEC_START_TS,
        VAL_EXEC_END_TS,
        VAL_EXEC_ELAPSE_SEC,
        VAL_EXEC_RESULT_TXT,
        VAL_EXEC_RESULT_CNT,
        VAL_EXEC_RESULT_THRSHLD_CNT,
        LAST_UPDT_TS,
        LAST_UPDT_USER_ID,
        LOAD_TS
    from {{ this }}
    {% if is_incremental() %}
    where 1=1
    {% else %}
    where 1=0
    {% endif %}
),

-- =========================================================
-- JOIN: Merge source with lookup
-- Reason: Performs lookup to identify insert vs update scenarios
-- =========================================================
joined_data as (
    select
        exp.DATA_SRC_VAL_ID,
        exp.DATA_SRC_ID,
        exp.VAL_RULE_ID,
        exp.VAL_EXEC_ID,
        exp.VAL_EXEC_STAT_CD,
        exp.VAL_EXEC_START_TS,
        exp.VAL_EXEC_END_TS,
        exp.VAL_EXEC_ELAPSE_SEC,
        exp.VAL_EXEC_RESULT_TXT,
        exp.VAL_EXEC_RESULT_CNT,
        exp.VAL_EXEC_RESULT_THRSHLD_CNT,
        exp.LAST_UPDT_TS,
        exp.LAST_UPDT_USER_ID,
        exp.CURR_TS,
        exp.CURR_DT,
        exp.DATA_SRC_VAL_STAGE_ID,
        lkp.DATA_SRC_VAL_STAGE_ID as LKP_DATA_SRC_VAL_STAGE_ID
    from exp_DATA_SRC_VAL_STAGE exp
    left join lkp_DATA_SRC_VAL_STAGE lkp
        on exp.DATA_SRC_VAL_ID = lkp.DATA_SRC_VAL_ID
),

-- =========================================================
-- UPDATE STRATEGY: exp_UPDATE_STRATEGY
-- Reason: Converted Informatica Update Strategy to dbt incremental logic with INSERT/UPDATE flags
-- =========================================================
exp_UPDATE_STRATEGY as (
    select
        DATA_SRC_VAL_STAGE_ID,
        DATA_SRC_VAL_ID,
        DATA_SRC_ID,
        VAL_RULE_ID,
        VAL_EXEC_ID,
        VAL_EXEC_STAT_CD,
        VAL_EXEC_START_TS,
        VAL_EXEC_END_TS,
        VAL_EXEC_ELAPSE_SEC,
        VAL_EXEC_RESULT_TXT,
        VAL_EXEC_RESULT_CNT,
        VAL_EXEC_RESULT_THRSHLD_CNT,
        LAST_UPDT_TS,
        LAST_UPDT_USER_ID,
        -- Reason: Replaced Informatica IIF with CASE for conditional logic
        case
            when LKP_DATA_SRC_VAL_STAGE_ID is null then CURR_TS
            else LAST_UPDT_TS
        end as LOAD_TS,
        -- Reason: Update strategy flag - INSERT (0) if lookup returns null, UPDATE (1) if exists
        case
            when LKP_DATA_SRC_VAL_STAGE_ID is null then 0
            else 1
        end as UPDATE_STRATEGY_FLAG
    from joined_data
)

-- =========================================================
-- FINAL SELECT: Target output
-- Reason: dbt models end with SELECT; materialization handles persistence
-- =========================================================
select
    DATA_SRC_VAL_STAGE_ID,
    DATA_SRC_VAL_ID,
    DATA_SRC_ID,
    VAL_RULE_ID,
    VAL_EXEC_ID,
    VAL_EXEC_STAT_CD,
    VAL_EXEC_START_TS,
    VAL_EXEC_END_TS,
    VAL_EXEC_ELAPSE_SEC,
    VAL_EXEC_RESULT_TXT,
    VAL_EXEC_RESULT_CNT,
    VAL_EXEC_RESULT_THRSHLD_CNT,
    LAST_UPDT_TS,
    LAST_UPDT_USER_ID,
    LOAD_TS
from exp_UPDATE_STRATEGY

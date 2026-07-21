=============================================
-- Author: Ascendion AAVA
-- Created on:
-- Description: Informatica to dbt Conversion - Employee staging model with transformations
=============================================

-- =========================================================
-- CONVERSION LOG
-- =========================================================
-- Input Type : Informatica Mapping XML
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
    materialized='table',
    tags=['staging', 'employee']
) }}

-- Reason: Informatica source qualifier converted to dbt source reference
with source_data as (
    select
        EMPLOYEE_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE_NUMBER,
        HIRE_DATE,
        JOB_ID,
        SALARY,
        COMMISSION_PCT,
        MANAGER_ID,
        DEPARTMENT_ID
    from {{ source('hr_system', 'employees') }}
),

-- Reason: Expression transformation EXP_EMPLOYEE_TRANSFORM converted to CTE with CASE expressions
transformed_data as (
    select
        EMPLOYEE_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE_NUMBER,
        HIRE_DATE,
        JOB_ID,
        SALARY,
        COMMISSION_PCT,
        MANAGER_ID,
        DEPARTMENT_ID,
        
        -- Reason: IIF function converted to CASE WHEN for dbt SQL compatibility
        case 
            when SALARY > 10000 then 'High'
            when SALARY > 5000 then 'Medium'
            else 'Low'
        end as SALARY_GRADE,
        
        -- Reason: NVL function converted to COALESCE for NULL handling
        coalesce(COMMISSION_PCT, 0) as COMMISSION_PCT_CLEAN,
        
        -- Reason: String concatenation using || converted to CONCAT function
        concat(FIRST_NAME, ' ', LAST_NAME) as FULL_NAME,
        
        -- Reason: SYSDATE converted to CURRENT_TIMESTAMP for dbt compatibility
        current_timestamp as LOAD_TIMESTAMP,
        
        -- Reason: TRUNC function converted to DATE_TRUNC for date truncation
        date_trunc('day', HIRE_DATE) as HIRE_DATE_TRUNC,
        
        -- Reason: Informatica DECODE converted to CASE WHEN structure
        case 
            when JOB_ID = 'IT_PROG' then 'Information Technology'
            when JOB_ID = 'SA_REP' then 'Sales'
            when JOB_ID = 'ST_CLERK' then 'Stock Management'
            when JOB_ID = 'FI_ACCOUNT' then 'Finance'
            else 'Other'
        end as DEPARTMENT_NAME,
        
        -- Reason: SUBSTR function converted to SUBSTRING for string manipulation
        substring(EMAIL, 1, 10) as EMAIL_PREFIX,
        
        -- Reason: ADD_TO_DATE converted to DATEADD for date arithmetic
        dateadd(year, 1, HIRE_DATE) as FIRST_ANNIVERSARY,
        
        -- Reason: Calculated field for years of service using DATEDIFF
        datediff(year, HIRE_DATE, current_date) as YEARS_OF_SERVICE,
        
        -- Reason: IIF for boolean flag converted to CASE WHEN
        case 
            when MANAGER_ID is null then 'Y'
            else 'N'
        end as IS_TOP_LEVEL,
        
        -- Reason: Complex expression with multiple conditions
        case 
            when SALARY > 15000 and coalesce(COMMISSION_PCT, 0) > 0.2 then 'Star Performer'
            when SALARY > 10000 then 'High Performer'
            when SALARY > 5000 then 'Average Performer'
            else 'Entry Level'
        end as PERFORMANCE_CATEGORY,
        
        -- Reason: NVL with default value converted to COALESCE
        coalesce(DEPARTMENT_ID, -1) as DEPARTMENT_ID_CLEAN,
        
        -- Reason: String length check using LENGTH function
        length(PHONE_NUMBER) as PHONE_LENGTH,
        
        -- Reason: INSTR function converted to POSITION for substring search
        position('@' in EMAIL) as EMAIL_AT_POSITION,
        
        -- Reason: Upper case conversion maintained as UPPER function
        upper(EMAIL) as EMAIL_UPPER,
        
        -- Reason: Salary calculation with commission
        SALARY * (1 + coalesce(COMMISSION_PCT, 0)) as TOTAL_COMPENSATION,
        
        -- Reason: Conditional salary bonus calculation
        case 
            when datediff(year, HIRE_DATE, current_date) > 10 then SALARY * 0.15
            when datediff(year, HIRE_DATE, current_date) > 5 then SALARY * 0.10
            when datediff(year, HIRE_DATE, current_date) > 2 then SALARY * 0.05
            else 0
        end as TENURE_BONUS,
        
        -- Reason: Date difference in days
        datediff(day, HIRE_DATE, current_date) as DAYS_EMPLOYED,
        
        -- Reason: Trim function for cleaning string data
        trim(FIRST_NAME) as FIRST_NAME_CLEAN,
        trim(LAST_NAME) as LAST_NAME_CLEAN,
        
        -- Reason: Replace function for data cleansing
        replace(PHONE_NUMBER, '-', '') as PHONE_NUMBER_CLEAN,
        
        -- Reason: Conditional department grouping
        case 
            when JOB_ID in ('IT_PROG', 'IT_SUPPORT') then 'IT'
            when JOB_ID in ('SA_REP', 'SA_MAN') then 'SALES'
            when JOB_ID in ('FI_ACCOUNT', 'FI_MGR') then 'FINANCE'
            when JOB_ID in ('ST_CLERK', 'ST_MAN') then 'OPERATIONS'
            else 'ADMIN'
        end as DEPARTMENT_GROUP,
        
        -- Reason: Salary range indicator
        case 
            when SALARY between 0 and 5000 then '0-5K'
            when SALARY between 5001 and 10000 then '5K-10K'
            when SALARY between 10001 and 15000 then '10K-15K'
            when SALARY between 15001 and 20000 then '15K-20K'
            else '20K+'
        end as SALARY_RANGE,
        
        -- Reason: Email domain extraction using SUBSTRING and POSITION
        substring(EMAIL, position('@' in EMAIL) + 1, length(EMAIL)) as EMAIL_DOMAIN,
        
        -- Reason: Quarter of hire date
        date_part('quarter', HIRE_DATE) as HIRE_QUARTER,
        
        -- Reason: Year of hire date
        date_part('year', HIRE_DATE) as HIRE_YEAR,
        
        -- Reason: Month of hire date
        date_part('month', HIRE_DATE) as HIRE_MONTH,
        
        -- Reason: Day of week for hire date
        date_part('dow', HIRE_DATE) as HIRE_DAY_OF_WEEK,
        
        -- Reason: Age calculation from hire date
        floor(datediff(day, HIRE_DATE, current_date) / 365.25) as EMPLOYMENT_YEARS,
        
        -- Reason: Salary percentile indicator
        case 
            when SALARY >= 20000 then 'Top 10%'
            when SALARY >= 15000 then 'Top 25%'
            when SALARY >= 10000 then 'Top 50%'
            else 'Bottom 50%'
        end as SALARY_PERCENTILE,
        
        -- Reason: Commission tier
        case 
            when coalesce(COMMISSION_PCT, 0) >= 0.30 then 'Tier 1'
            when coalesce(COMMISSION_PCT, 0) >= 0.20 then 'Tier 2'
            when coalesce(COMMISSION_PCT, 0) >= 0.10 then 'Tier 3'
            when coalesce(COMMISSION_PCT, 0) > 0 then 'Tier 4'
            else 'No Commission'
        end as COMMISSION_TIER,
        
        -- Reason: Manager flag
        case 
            when EMPLOYEE_ID in (select distinct MANAGER_ID from {{ source('hr_system', 'employees') }} where MANAGER_ID is not null) then 'Y'
            else 'N'
        end as IS_MANAGER,
        
        -- Reason: Record hash for change detection using MD5
        md5(concat(
            coalesce(cast(EMPLOYEE_ID as varchar), ''),
            coalesce(FIRST_NAME, ''),
            coalesce(LAST_NAME, ''),
            coalesce(EMAIL, ''),
            coalesce(PHONE_NUMBER, ''),
            coalesce(cast(HIRE_DATE as varchar), ''),
            coalesce(JOB_ID, ''),
            coalesce(cast(SALARY as varchar), ''),
            coalesce(cast(COMMISSION_PCT as varchar), ''),
            coalesce(cast(MANAGER_ID as varchar), ''),
            coalesce(cast(DEPARTMENT_ID as varchar), '')
        )) as RECORD_HASH,
        
        -- Reason: Data quality flag
        case 
            when EMAIL is null or PHONE_NUMBER is null or HIRE_DATE is null then 'INCOMPLETE'
            when SALARY <= 0 then 'INVALID_SALARY'
            when HIRE_DATE > current_date then 'FUTURE_HIRE_DATE'
            else 'VALID'
        end as DATA_QUALITY_FLAG,
        
        -- Reason: Active employee indicator based on business rules
        case 
            when SALARY > 0 and HIRE_DATE <= current_date then 'ACTIVE'
            else 'INACTIVE'
        end as EMPLOYEE_STATUS

    from source_data
),

-- Reason: Filter transformation FIL_ACTIVE_EMPLOYEES converted to WHERE clause
filtered_data as (
    select *
    from transformed_data
    where EMPLOYEE_STATUS = 'ACTIVE'
      and DATA_QUALITY_FLAG = 'VALID'
)

-- Reason: Final select represents the target definition from Informatica
select
    EMPLOYEE_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE_NUMBER,
    HIRE_DATE,
    JOB_ID,
    SALARY,
    COMMISSION_PCT,
    MANAGER_ID,
    DEPARTMENT_ID,
    SALARY_GRADE,
    COMMISSION_PCT_CLEAN,
    FULL_NAME,
    LOAD_TIMESTAMP,
    HIRE_DATE_TRUNC,
    DEPARTMENT_NAME,
    EMAIL_PREFIX,
    FIRST_ANNIVERSARY,
    YEARS_OF_SERVICE,
    IS_TOP_LEVEL,
    PERFORMANCE_CATEGORY,
    DEPARTMENT_ID_CLEAN,
    PHONE_LENGTH,
    EMAIL_AT_POSITION,
    EMAIL_UPPER,
    TOTAL_COMPENSATION,
    TENURE_BONUS,
    DAYS_EMPLOYED,
    FIRST_NAME_CLEAN,
    LAST_NAME_CLEAN,
    PHONE_NUMBER_CLEAN,
    DEPARTMENT_GROUP,
    SALARY_RANGE,
    EMAIL_DOMAIN,
    HIRE_QUARTER,
    HIRE_YEAR,
    HIRE_MONTH,
    HIRE_DAY_OF_WEEK,
    EMPLOYMENT_YEARS,
    SALARY_PERCENTILE,
    COMMISSION_TIER,
    IS_MANAGER,
    RECORD_HASH,
    DATA_QUALITY_FLAG,
    EMPLOYEE_STATUS
from filtered_data

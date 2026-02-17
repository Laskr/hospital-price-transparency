with raw_source as (
    select * from {{ source('hospital_raw', 'RAW_BANNER_270036499_BANNER_UNIVERSITY_MEDICAL_CENTER_PHOENIX_STANDARDCHARGES') }}
),

renamed as (
    select
        -- Extracting from the JSON 'src_data' column we created in Python
        src_data:"0"::string as description,
        src_data:"1"::string as code_1,
        src_data:"2"::string as code_1_type,
        -- ... and so on ...
        ingestion_timestamp
    from raw_source
)

select * from renamed
-- Filter out the header rows that are stuck in the data
where description not in ('hospital_name', 'description')
  and description is not null

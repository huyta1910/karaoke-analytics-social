{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'page_insights_daily') }}
),

renamed as (
    select
        date as metric_date,
        metric_name,
        value as metric_value,
        ingestion_time,
        -- Add row number to handle potential duplicates
        row_number() over (
            partition by date, metric_name 
            order by ingestion_time desc
        ) as row_num
    from source
)

-- Keep only the most recent ingestion for each date/metric combination
select
    metric_date,
    metric_name,
    metric_value,
    ingestion_time
from renamed
where row_num = 1

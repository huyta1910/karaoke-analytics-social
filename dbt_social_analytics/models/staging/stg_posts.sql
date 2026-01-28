{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'post_performance') }}
),

renamed as (
    select
        post_id,
        created_at as posted_at,
        message as post_message,
        post_url,
        impressions,
        engaged_users,
        likes,
        ingestion_time,
        date(created_at) as post_date,
        extract(hour from created_at) as post_hour,
        extract(dayofweek from created_at) as day_of_week,
        row_number() over (
            partition by post_id 
            order by ingestion_time desc
        ) as row_num
    from source
)

select
    post_id,
    posted_at,
    post_message,
    post_url,
    impressions,
    engaged_users,
    likes,
    ingestion_time,
    post_date,
    post_hour,
    day_of_week
from renamed
where row_num = 1

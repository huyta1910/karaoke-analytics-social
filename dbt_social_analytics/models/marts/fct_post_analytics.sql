{{
    config(
        materialized='table',
        partition_by={
            "field": "post_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with posts as (
    select * from {{ ref('stg_posts') }}
),

enriched as (
    select
        post_id,
        posted_at,
        post_message,
        post_url,
        impressions,
        engaged_users,
        likes,
        post_date,
        post_hour,
        day_of_week,
        ingestion_time,
        
        -- Engagement metrics
        case 
            when impressions > 0 
            then round(safe_divide(engaged_users, impressions) * 100, 2)
            else 0 
        end as engagement_rate_pct,
        
        case 
            when impressions > 0 
            then round(safe_divide(likes, impressions) * 100, 2)
            else 0 
        end as like_rate_pct,
        
        -- Day name for readability
        case day_of_week
            when 1 then 'Sunday'
            when 2 then 'Monday'
            when 3 then 'Tuesday'
            when 4 then 'Wednesday'
            when 5 then 'Thursday'
            when 6 then 'Friday'
            when 7 then 'Saturday'
        end as day_name,
        
        -- Time of day bucket
        case 
            when post_hour between 6 and 11 then 'Morning'
            when post_hour between 12 and 17 then 'Afternoon'
            when post_hour between 18 and 21 then 'Evening'
            else 'Night'
        end as time_of_day,

        -- Rolling averages (last 7 posts)
        avg(impressions) over (
            order by posted_at 
            rows between 6 preceding and current row
        ) as avg_impressions_7_posts,
        
        avg(engaged_users) over (
            order by posted_at 
            rows between 6 preceding and current row
        ) as avg_engaged_users_7_posts

    from posts
)

select * from enriched
order by posted_at desc

create or replace table `agency_data_pipeline`.`landing_page_by_month`
  
  as (
    SELECT
yyyymm, 
site,
platform,
channel, 
url,
cost,
impressions,
clicks,
conversions,
sessions,
sum(sessions) OVER w1 total_sessions,
goal_completions,
sum(goal_completions) OVER w1 total_goal_completions
FROM (
    SELECT 
    FORMAT_DATE("%Y-%m", date) AS yyyymm,
    site,
    platform,
    channel, 
    url,
    sum(cost) cost,
    sum(impressions) impressions,
    sum(clicks) clicks,
    sum(conversions) conversions,
    sum(sessions) sessions,
    sum(goal_completions) goal_completions
    FROM `adp-apprenticeship`.`agency_data_pipeline`.`agg_platforms_ga`
    GROUP BY yyyymm, site, platform, channel, url
)
WINDOW w1 as (PARTITION BY yyyymm, site)
  );

    
create or replace table `agency_data_pipeline`.`agg_platforms_union`
  
  as (
    SELECT 
date, 
account, 
channel,
platform,
url,
cost,
impressions,
clicks,
conversions
FROM 
`adp-apprenticeship`.`agency_data_pipeline`.`adwords_stats`

UNION ALL  

SELECT 
date, 
account, 
channel,
platform,
url,
cost,
impressions,
clicks,
conversions
FROM 
`adp-apprenticeship`.`agency_data_pipeline`.`fb_ads_stats`

UNION ALL

SELECT 
date, 
account, 
channel,
platform,
url,
cost,
impressions,
clicks,
conversions
FROM 
`adp-apprenticeship`.`agency_data_pipeline`.`gsc_stats`

UNION ALL

SELECT 
date, 
account, 
channel,
platform,
url,
cost,
impressions,
clicks,
conversions
FROM 
`adp-apprenticeship`.`agency_data_pipeline`.`youtube_stats`
  );

    
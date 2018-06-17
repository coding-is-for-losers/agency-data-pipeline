create or replace table `agency_data_pipeline`.`adwords_join`
  
  as (
    SELECT
day,
a.campaign,
a.campaignid, 
account, 
channel,
platform,
b.url,
cost,
impressions,
clicks,
conversions
FROM `adp-apprenticeship`.`agency_data_pipeline`.`adwords_campaigns` a
LEFT JOIN `adp-apprenticeship`.`agency_data_pipeline`.`adwords_urls` b
ON a.campaignid = b.campaignid
  );

    
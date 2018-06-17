create or replace table `agency_data_pipeline`.`agg_platforms_site`
  
  as (
    SELECT 
date, 
a.account account, 
b.site site,
a.platform platform,
a.channel channel,
url,
cost,
impressions,
clicks,
conversions
FROM 
  `adp-apprenticeship`.`agency_data_pipeline`.`agg_platforms_union` a
LEFT JOIN 
  `adp-apprenticeship`.`agency_data_pipeline`.`accounts_proc` b
ON ( 
  a.account = b.account AND
  a.platform = b.platform
)
  );

    
SELECT
date, 
account, 
channel,
platform,
url,
0 as cost,
sum(impressions) as impressions,
sum(clicks) as clicks,
0 as conversions
FROM 
  ( 
	SELECT  
	date, 
	site as account,
	'Organic' as channel,
	'Organic' as platform,
	lower(trim(regexp_replace(replace(replace(replace(replace(landing_page_url,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url,
	max(impressions) as impressions, 
	max(clicks) as clicks
	FROM `{{ target.project }}.agency_data_pipeline.gsc`
	GROUP BY date, account, channel, platform, url
  ) 
GROUP BY date, account, channel, platform, url
ORDER BY account asc, date asc, url asc
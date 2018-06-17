SELECT
date, 
account, 
channel,
platform,
url,
sum(cost) as cost,
sum(impressions) as impressions,
sum(clicks) as clicks,
sum(conversions) as conversions
FROM 
  ( 
	SELECT  
	date, 
	'Social' as channel,
	'YouTube' as platform,
	'Coding is for Losers' as account,  
	0 as cost, 
	max(views) as impressions, 
	max(clicks) as clicks, 
	max(subscribers) as conversions,
	regexp_replace(regexp_replace(replace(replace(replace(landing_page_url,'http://',''),'https://',''),'.html',''),r'\?.*$',''),r'([/]$)','') as url,
	video_url
	FROM `adp-apprenticeship.agency_data_pipeline.youtube`
	group by channel, platform, account, date, video_url, url
  ) 
GROUP BY date, account, channel, platform, url
SELECT
campaignid,
lower(trim(regexp_replace(replace(replace(replace(replace(finalurl,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url
FROM 
(
	SELECT  
	campaignid,
	max(finalurl) finalurl
	FROM `adp-apprenticeship.adwords.FINAL_URL_REPORT`
	GROUP BY campaignid
)
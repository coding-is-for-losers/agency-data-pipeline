



with fb_adcreative as (


	    
		   	SELECT 
			id,
			link_url object_url,
			url_tags,
			_sdc_sequence
			FROM `adp-apprenticeship.fb_ads_cifl.adcreative`
		    
	   
)

select creative_id, max(url) url
from (
  select
  id creative_id,
  lower(trim(regexp_replace(replace(replace(replace(replace(object_url,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url,
  first_value(_sdc_sequence) OVER (PARTITION BY id ORDER BY _sdc_sequence DESC) lv,
  _sdc_sequence
  from fb_adcreative
  )
where lv = _sdc_sequence
group by creative_id


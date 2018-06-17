create or replace table `agency_data_pipeline`.`fb_ads`
  
  as (
    -- depends_on: `adp-apprenticeship`.`agency_data_pipeline`.`fb_adcreative`





with fb_ads as (

	    
		   	SELECT 
			id ad_id,
			creative.creative_id creative_id,
			_sdc_sequence
			FROM `adp-apprenticeship.fb_ads_cifl.ads`
		     
	   

)

SELECT
a.ad_id,
b.creative_id,
b.url
FROM (
    SELECT
    ad_id,
    creative_id,
    first_value(_sdc_sequence) OVER (PARTITION BY ad_id ORDER BY _sdc_sequence DESC) lv,
    _sdc_sequence
    FROM fb_ads
    ) a
LEFT JOIN `adp-apprenticeship`.`agency_data_pipeline`.`fb_adcreative` b
ON (
	a.creative_id = b.creative_id 
)
WHERE a.lv = a._sdc_sequence


  );

    
  -- depends_on: {{ ref('fb_adcreative') }}

{% set accounts = get_column_values(table=ref('accounts_proc'), column='bigquery_name', max_records=50, filter_column='platform', filter_value='FB Ads') %}

{% if accounts != [] %}

with fb_ads as (

	    {% for account in accounts %}
		   	SELECT 
			id ad_id,
			creative.creative_id creative_id,
			_sdc_sequence
			FROM `{{ target.project }}.fb_ads_{{account}}.ads`
		     {% if not loop.last %} UNION ALL {% endif %}
	   {% endfor %}

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
LEFT JOIN {{ref('fb_adcreative')}} b
ON (
	a.creative_id = b.creative_id 
)
WHERE a.lv = a._sdc_sequence

{% endif %}

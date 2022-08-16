-----INT Weekly Recipes-----

WITH current_week as(
  SELECT distinct (hellofresh_running_week) as current_running_week FROM dimensions.date_dimension
  WHERE date_string_backwards = to_date(now())
  )
, menu AS (
  SELECT DISTINCT cps_menu.id as menu_id,
     cps_menu.region_code as countrymenu,
     cps_menu.hellofresh_week,
     cps_menu.slot_number,
     cps_menu.recipe_id,
     cps_recipe.id,
     cps_recipe.unique_recipe_code,
     cps_recipe.recipe_code,
     cps_recipe.version,
     cps_recipe.status,
     cps_recipe.title,
     cps_recipe.subtitle,
     cps_recipe.recipe_type,
     cps_recipe.brand,
     cps_recipe.target_preferences,
     cps_recipe.target_products,
     cps_recipe.tags,
     cps_recipe.label,
     cps_recipe.difficulty,
     cps_recipe.cooking_methods,
     cps_recipe.cuisine,
     cps_recipe.dish_type,
     cps_recipe.spiciness,
     cps_recipe.primary_protein,
     cps_recipe.primary_vegetable,
     cps_recipe.primary_starch,
     cps_recipe.primary_cheese,
     cps_recipe.primary_dairy,
     cps_recipe.primary_fruit,
     cps_recipe.main_protein,
     cps_recipe.protein_cut,
     cps_recipe.used_week,
     cps_recipe.total_time,
     cps_recipe.hands_off_time,
     cps_recipe.hands_on_time,
     cps_recipe.active_cooking_time,
     cps_recipe.prep_time,
     cps_recipe.main_image,
     cps_recipe.image_url
     --cps_recipe.*,
     --sp.distribution_center as dc_name
  FROM materialized_views.isa_services_menu AS cps_menu
  LEFT JOIN materialized_views.isa_services_recipe_consolidated AS cps_recipe
    ON cps_menu.recipe_id = cps_recipe.id
  LEFT JOIN materialized_views.culinary_services_recipe_static_price AS sp
    ON cps_recipe.id = sp.recipe_id
  WHERE cps_menu.region_code in ('se', 'dk', 'it', 'jp', 'no')
    --AND sp.distribution_center in ('SK','MO')
  AND cps_menu.hellofresh_week >= '2022-W33' AND cps_menu.hellofresh_week <= '2022-W60'/*in (
    SELECT DISTINCT hellofresh_week
    FROM dimensions.date_dimension
    WHERE hellofresh_running_week >= (SELECT current_running_week + 5 from current_week limit 1)
    AND hellofresh_running_week <= (select current_running_week + 10 from current_week limit 1) )*/
  )
, skus AS (
  SELECT menu.id,
     menu.hellofresh_week,
     skus.code,
     skus.id AS skuid,
     skus.name,
     skus.packaging_size,
     skus.packaging_quantity,
     skus.status AS sku_status,
     CASE WHEN recipe_skus.size = 2 THEN servings_ratio ELSE NULL END as quantity_used_2p,
     CASE WHEN recipe_skus.size = 2 THEN servings_ratio ELSE NULL END as quantity_used_4p,
     CASE WHEN recipe_skus.size = 2 THEN pick_count ELSE NULL END as quantity_to_order_2p,
     CASE WHEN recipe_skus.size = 4 THEN pick_count ELSE NULL END as quantity_to_order_4p
  FROM menu
  LEFT JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku as recipe_skus
    ON menu.recipe_id = recipe_skus.recipe_id
    AND recipe_skus.market IN ('gb','it','jp','dkse','no')
  LEFT JOIN materialized_views.procurement_services_culinarysku as skus
    ON skus.id = recipe_skus.culinarysku_id
    AND skus.market IN ('gb','it','jp','dkse','no')
  )

, inactiveskus as (
  SELECT skus.id,
     COUNT(DISTINCT skus.code) AS inact_skus
     --group_concat(distinct(skus.code), " | ") AS inact_skus
  FROM skus
  WHERE skus.sku_status in ('Inactive','Archived')
  GROUP BY skus.id--, skus.code
  )

, donotuseskus as (
  SELECT skus.id,
     COUNT(DISTINCT skus.code) AS do_not_use_skus
     --group_concat(distinct(skus.code), " | ") as do_not_use_skus
  FROM skus
  WHERE skus.name LIKE '%DO NOT USE%'
  GROUP BY skus.id
  )
, skuinfo as (
  SELECT distinct skus.id,
          skus.code,
          skus.name
  FROM skus
  )
, allskus as (
  SELECT skuinfo.id,
     group_concat(skuinfo.code," | ") AS skucodes,
     group_concat(skuinfo.name," | ") AS skunames,
     group_concat(concat(skuinfo.code, ": ", skuinfo.name)," | ") as skunames_and_codes_procurement
  FROM skuinfo
  GROUP BY skuinfo.id
  )
, all_sku_sequencing AS (
  SELECT menu.id,
     menu.hellofresh_week,
     CEILING(SUM(CASE WHEN recipe_skus.size = 2 THEN servings_ratio ELSE NULL END * skus.packaging_size)) AS quantity_2p,
     CEILING(SUM(CASE WHEN recipe_skus.size = 4 THEN servings_ratio ELSE NULL END * skus.packaging_size)) AS quantity_4p,
     SUM(CASE WHEN recipe_skus.size = 2 THEN pick_count ELSE NULL END) as order_2p,
     SUM(CASE WHEN recipe_skus.size = 4 THEN pick_count ELSE NULL END) as order_4p
  FROM menu
  LEFT JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku recipe_skus
    ON menu.recipe_id = recipe_skus.recipe_id
    AND recipe_skus.market IN ('gb','it','jp','dkse','no')
  LEFT JOIN materialized_views.procurement_services_culinarysku skus
    ON skus.id = recipe_skus.culinarysku_id
    AND skus.market IN ('gb','it','jp','dkse','no')
  GROUP BY menu.id, menu.hellofresh_week
  )

, nutrition as(
  select *
  from materialized_views.culinary_services_recipe_segment_nutrition
  where market IN ('dkse', 'it', 'gb', 'jp', 'no')
    --and segment in ('SE')
  )

, spicysku as (
  SELECT skus.id,
     group_concat(distinct(skus.name)," | ") AS spicy_sku
  FROM skus
  WHERE skus.name LIKE '%chili / chili /chili/ Chili%'
    OR skus.name LIKE '%chili%'
    OR skus.name LIKE '%Chili%'
    OR skus.name LIKE '%chilli%'
    OR skus.name LIKE '%Sriracha sauce%'
    OR skus.name LIKE '%sriracha%'
    OR skus.name LIKE '%Jalapeno, Green, Medium Spicy%'
    OR skus.name LIKE '%jalapeno%'
    OR skus.name LIKE '%Sriracha Mayo%'
    OR skus.name LIKE '%Chorizo Sausage%'
    OR skus.name LIKE '%Chili, Dried%'
    OR skus.name LIKE '%wasabi%'
    OR skus.name LIKE '%karashi%'
  GROUP BY skus.id
  )

, Almost_all as (
SELECT CASE WHEN menu.countrymenu = 'dk' THEN 'DK'
    WHEN menu.countrymenu = 'no' THEN 'NO'
    WHEN menu.countrymenu = 'se' THEN 'SE'
    WHEN menu.countrymenu = 'it' THEN 'IT'
    WHEN menu.countrymenu = 'gb' THEN 'GB'
    WHEN menu.countrymenu = 'jp' THEN 'JP'
    ELSE menu.countrymenu END AS country,
   menu.hellofresh_week AS hf_week,
   menu.slot_number AS slot,
   isnull(menu.unique_recipe_code,"0") AS uniquerecipecode,
   isnull(menu.recipe_code,"0") AS recipecode,
   isnull(menu.title,"0") AS title,
   isnull(menu.subtitle,"0") AS subtitle,
   isnull(menu.status,"0") AS status,
   isnull(CAST(menu.hands_on_time AS int), 0) AS handsontime,
   CASE WHEN menu.hands_on_time ="" OR menu.hands_on_time IS NULL THEN cast(99 as float)
        ELSE cast(menu.hands_on_time as float) END
         +
       case when menu.hands_off_time ="" or menu.hands_off_time is NULL then cast(99 as float)
        else cast(menu.hands_off_time as float) end
         AS totaltime,
   isnull(menu.image_url,'0') AS imageurl,
   isnull(menu.dish_type,"0") AS dishtype,
   isnull(menu.cuisine,"0") AS cuisine,
   isnull(n.energy,0) AS kilo_calories,
   --isnull(n.salt,0) AS salt,
   isnull(menu.target_preferences,"0") AS preference,
   inactiveskus.inact_skus AS inactiveskus,
   donotuseskus.do_not_use_skus AS donotuseskus,
   isnull(allskus.skunames,"0") AS skunames,
   isnull(allskus.skucodes,"0") AS skucodes,
   isnull(allskus.skunames_and_codes_procurement,"0") AS skunames_and_codes_procurement,
   menu.tags AS recipe_tags,
   menu.recipe_id
FROM menu
LEFT JOIN nutrition n
  ON menu.recipe_id = n.recipe_id
LEFT JOIN inactiveskus
  ON menu.recipe_id = inactiveskus.id
LEFT JOIN allskus
  ON menu.recipe_id = allskus.id
LEFT JOIN donotuseskus
  ON menu.recipe_id = donotuseskus.id
  )

SELECT DISTINCT
   CASE WHEN country = 'DK' THEN 'DKSE' ELSE country END AS country,
   hf_week,
   slot,
   uniquerecipecode,
   recipecode,
   title,
   subtitle,
   status,
   --handsontime,
   --totaltime,
   --dishtype,
   --cuisine
   --kilo_calories,
   --salt,
   --preference,
   --inactiveskus,
   --donotuseskus,
   --lower(skunames) as skunames,
   --skucodes,
   --skunames_and_codes_procurement,
   --recipe_tags,
   --spicy_sku,
   CASE WHEN imageurl IS NULL THEN "https://cdn.mos.cms.futurecdn.net/oYpPSTxAmoJg3FkhdwbiF3.jpg" ELSE imageurl END AS imageurl
FROM Almost_all
LEFT JOIN spicysku
  ON Almost_all.recipe_id = spicysku.id
WHERE country <> 'SE'
ORDER BY 2,3,4,5

UNION

----- GB Planned Recipes -----

SELECT  r.country
        ,  m.yearweek as hf_week
        , m.slotnumber as slot
        , r.uniquerecipecode
        , r.mainrecipecode AS recipecode
        , CASE
            WHEN r.subtitle is null then r.title
            ELSE CONCAT(r.title, ' ' , r.subtitle)
            END as title
        , r.subtitle
        , r.status
        , CASE WHEN r.mainimageurl IS NULL THEN "https://cdn.mos.cms.futurecdn.net/oYpPSTxAmoJg3FkhdwbiF3.jpg" ELSE r.mainimageurl END AS imageurl
    FROM materialized_views.int_scm_analytics_remps_recipe r
        LEFT JOIN materialized_views.int_scm_analytics_remps_menu m
            ON r.id = m.slot_recipe
            AND r.country = m.country
            AND m.product in ('classic-box', 'modularity')
    WHERE r.country = 'GB'
    AND m.yearweek >= '2022-W33' AND m.yearweek <= '2022-W60'
    AND product NOT LIKE 'modularity'
ORDER BY 1,2


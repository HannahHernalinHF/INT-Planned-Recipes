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
  AND cps_menu.hellofresh_week >= '2022-W37' AND cps_menu.hellofresh_week <= '2022-W65'/*in (
    SELECT DISTINCT hellofresh_week
    FROM dimensions.date_dimension
    WHERE hellofresh_running_week >= (SELECT current_running_week + 5 from current_week limit 1)
    AND hellofresh_running_week <= (select current_running_week + 10 from current_week limit 1) )*/
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
   /*isnull(CAST(menu.hands_on_time AS int), 0) AS handsontime,
   CASE WHEN menu.hands_on_time ="" OR menu.hands_on_time IS NULL THEN cast(99 as float)
        ELSE cast(menu.hands_on_time as float) END
         +
       case when menu.hands_off_time ="" or menu.hands_off_time is NULL then cast(99 as float)
        else cast(menu.hands_off_time as float) end
         AS totaltime,*/
   isnull(menu.image_url,'0') AS imageurl,
   /*isnull(menu.dish_type,"0") AS dishtype,
   isnull(menu.cuisine,"0") AS cuisine,
   isnull(n.energy,0) AS kilo_calories,
   --isnull(n.salt,0) AS salt,
   isnull(menu.target_preferences,"0") AS preference,
   inactiveskus.inact_skus AS inactiveskus,
   donotuseskus.do_not_use_skus AS donotuseskus,
   isnull(allskus.skunames,"0") AS skunames,
   isnull(allskus.skucodes,"0") AS skucodes,
   isnull(allskus.skunames_and_codes_procurement,"0") AS skunames_and_codes_procurement,*/
   menu.tags AS recipe_tags,
   menu.recipe_id
FROM menu
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
WHERE country <> 'SE'

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
    WHERE r.country = 'GB' AND m.yearweek >= '2022-W37' AND m.yearweek <= '2022-W65' AND r.uniquerecipecode NOT LIKE '%MOD%' AND r.status <> 'Menu Gap Filler' AND r.title NOT LIKE '%Gap Filler%' AND r.title NOT LIKE '%MODULARITY GAP%'  AND r.title NOT LIKE '%culinary gap%' AND r.title NOT LIKE '%Culinary Gap%'
    --AND product NOT LIKE 'modularity' OR r.status NOT LIKE 'Menu Gap Filler'
    --AND r.uniquerecipecode NOT LIKE 'AO-%'


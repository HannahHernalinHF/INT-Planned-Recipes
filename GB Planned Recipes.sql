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

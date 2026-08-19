

-- DT_DAILY_REVENUE
CREATE OR REPLACE DYNAMIC TABLE SHOPFLOW_DB.MARTS.DT_DAILY_REVENUE
    TARGET_LAG = '5 MINUTE'
    WAREHOUSE = WH_INGEST
AS
SELECT 
    DATE(stg_o.order_date) as jour,
    SUM(stg_oi.quantity * stg_oi.unit_price) as ca_jour,
    COUNT(DISTINCT stg_o.order_id) as nb_commandes_jour
FROM SHOPFLOW_DB.STAGING.STG_ORDERS as stg_o
LEFT JOIN SHOPFLOW_DB.STAGING.STG_ORDER_ITEMS as stg_oi ON stg_o.order_id = stg_oi.order_id
GROUP BY 1
ORDER BY 1;



-- DT_TOP_PRODUCTS
CREATE OR REPLACE DYNAMIC TABLE SHOPFLOW_DB.MARTS.DT_TOP_PRODUCTS
    TARGET_LAG = '5 MINUTE'
    WAREHOUSE = WH_INGEST
AS
SELECT 
    stg_oi.product_id as product_id,
    stg_p.name as product_name,
    SUM(stg_oi.quantity * stg_oi.unit_price) as total_price
FROM SHOPFLOW_DB.STAGING.STG_ORDERS as stg_o
LEFT JOIN SHOPFLOW_DB.STAGING.STG_ORDER_ITEMS as stg_oi ON stg_oi.order_id = stg_o.order_id 
LEFT JOIN SHOPFLOW_DB.STAGING.STG_PRODUCTS as stg_p ON stg_p.product_id = stg_oi.product_id
WHERE stg_o.order_date >= DATE('2026-09-15') - 30 -- Date fixe pour faciliter les Tests 
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 20;


-- DT_CUSTOMER_COHORTS
CREATE OR REPLACE DYNAMIC TABLE SHOPFLOW_DB.MARTS.DT_CUSTOMER_COHORTS
    TARGET_LAG = '5 MINUTE'
    WAREHOUSE = WH_INGEST
AS

WITH cohortes AS (
    SELECT 
        TO_CHAR(c.signup_date, 'YYYY-MM') AS mois_inscription,
        COUNT(DISTINCT c.customer_id) AS inscrits_depart
    FROM SHOPFLOW_DB.STAGING.STG_CUSTOMERS c
    WHERE c.signup_date IS NOT NULL
    GROUP BY 1
),

activite_clients AS (
    SELECT 
        TO_CHAR(c.signup_date, 'YYYY-MM') AS mois_inscription,
        o.customer_id
    FROM SHOPFLOW_DB.STAGING.STG_CUSTOMERS c
    JOIN SHOPFLOW_DB.STAGING.STG_ORDERS o 
      ON c.customer_id = o.customer_id
    WHERE c.signup_date IS NOT NULL
      AND o.order_date IS NOT NULL
)

SELECT 
    coh.mois_inscription,
    coh.inscrits_depart,
    COUNT(DISTINCT act.customer_id) AS clients_actifs,
    ROUND(COUNT(DISTINCT act.customer_id) / coh.inscrits_depart * 100, 1) AS taux_retention
FROM cohortes coh
JOIN activite_clients act 
  ON coh.mois_inscription = act.mois_inscription
GROUP BY 
    1,2
ORDER BY 
    1;





---- TIME TRAVEL A testser Demain

-- Suppression de lignes 
DELETE FROM SHOPFLOW_DB.RAW.ORDERS_RAW 
WHERE order_id IN (
    SELECT order_id 
    FROM SHOPFLOW_DB.RAW.ORDERS_RAW 
    LIMIT 1000
);

-- Consulter l'état de la table 60 secondes en arrière
SELECT COUNT(*) FROM SHOPFLOW_DB.RAW.ORDERS_RAW AT(OFFSET => -60);


-- Rétablissement des données 
INSERT INTO SHOPFLOW_DB.RAW.ORDERS_RAW
SELECT * 
FROM SHOPFLOW_DB.RAW.ORDERS_RAW AT(OFFSET => -60)
WHERE order_id NOT IN (
    SELECT order_id 
    FROM SHOPFLOW_DB.RAW.ORDERS_RAW
);
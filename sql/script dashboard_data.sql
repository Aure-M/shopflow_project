--- Ligne : CA par jour (14 derniers jours)

SELECT * 
FROM SHOPFLOW_DB.MARTS.DT_DAILY_REVENUE
ORDER BY JOUR DESC
LIMIT 14;


--- Barres : Top 10 produits

SELECT * 
FROM SHOPFLOW_DB.MARTS.DT_TOP_PRODUCTS
LIMIT 10;


--- Table : dernières commandes traitées

SELECT
    stg_o.order_date as order_date,
    stg_o.status as order_status,
    stg_o.total_amount as order_total_amount,
    stg_c.first_name || ' ' || stg_c.last_name as customer
FROM SHOPFLOW_DB.STAGING.STG_ORDERS as stg_o
JOIN SHOPFLOW_DB.STAGING.STG_CUSTOMERS as stg_c ON stg_c.customer_id  = stg_o.customer_id
ORDER BY stg_o.order_date DESC
LIMIT 20;

--- KPI : nombre de commandes du jour, panier moyen

------- Nombre de Commandes du jour
SELECT 
    COUNT(DISTINCT order_id) AS nb_orders
FROM SHOPFLOW_DB.STAGING.STG_ORDERS
GROUP BY DATE(order_date)   
ORDER BY DATE(order_date) DESC
LIMIT 1;


------- Panier Moyen
SELECT 
    AVG(total_amount) as acg_cart
FROM SHOPFLOW_DB.STAGING.STG_ORDERS;

# ShopFlow — Pipeline analytique e-commerce end-to-end

## Contexte

ShopFlow est une boutique e-commerce française spécialisée dans la vente de produits high-tech. Ce projet met en place une plateforme data Snowflake complète, de l'ingestion des fichiers bruts jusqu'à la restitution analytique, en passant par un pipeline automatisé (Streams, Tasks, Dynamic Tables).

## Architecture

```
┌──────────────────────────────┐
│   Stage interne              │
│   RAW.STAGE_LANDING          │
│   (CSV, Parquet, JSON)       │
└──────────────┬───────────────┘
               │ COPY INTO
               ▼
┌──────────────────────────────┐
│   SCHEMA : RAW               │
│   Tables brutes, VARIANT     │
└──────────────┬───────────────┘
               │ STREAM (CDC)
               ▼
┌──────────────────────────────┐
│   SCHEMA : STAGING           │
│   Nettoyage, typage,         │
│   parsing VARIANT → colonnes │
└──────────────┬───────────────┘
               │ Dynamic Tables
               ▼
┌──────────────────────────────┐
│   SCHEMA : MARTS             │
│   DT_DAILY_REVENUE           │
│   DT_TOP_PRODUCTS            │
│   DT_CUSTOMER_COHORTS        │
└──────────────────────────────┘
```

## Prérequis Snowflake

- Un compte Snowflake avec le rôle `ACCOUNTADMIN` (pour la création initiale)
- Warehouses créés par le script : `WH_INGEST` (XSMALL), `WH_TRANSFORM` (SMALL)
- Rôle applicatif : `SHOPFLOW_ENGINEER`

## Structure du dépôt

```
ShopflowProject/
├── Readme.md
├── script 01_setup_and_ingest.sql
├── script 02_semi_structured.sql.sql      ← NE PAS EXÉCUTER (contenu intégré dans le script 01)
├── script 03_streams_tasks.sql.sql
├── script 04_marts_and_timetravel.sql.sql
├── script dashboard_data.sql
└── script suspension_ressources.sql
```

## Instructions d'exécution

Exécuter les scripts **dans cet ordre** :

| # | Script | Rôle | Description |
|---|--------|------|-------------|
| 1 | `script 01_setup_and_ingest.sql` | ACCOUNTADMIN puis SHOPFLOW_ENGINEER | Création de la base, schémas, warehouses, rôle, stage, file formats. Ingestion de **tous** les fichiers (CSV, Parquet et JSON, y compris le semi-structuré). |
| 2 | `script 03_streams_tasks.sql.sql` | SHOPFLOW_ENGINEER | Création des Streams sur les tables RAW, tables STAGING, Tasks planifiées et chaînées. |
| 3 | `script 04_marts_and_timetravel.sql.sql` | SHOPFLOW_ENGINEER | Création des Dynamic Tables (MARTS) et exercice Time Travel (suppression + restauration). |
| 4 | `script dashboard_data.sql` | SHOPFLOW_ENGINEER | Requêtes analytiques de restitution (CA/jour, Top produits, dernières commandes, KPIs). |

> **Note :** Le `script 02_semi_structured.sql.sql` **ne doit pas être exécuté**. Son contenu (ingestion JSON, external table, parsing VARIANT) a été directement intégré dans le script 01 lors du développement.

## Dashboard

La fonctionnalité native de Dashboard Snowsight ayant été **dépréciée et retirée** par Snowflake (BCR-2260, juin 2026), les requêtes de restitution sont regroupées dans `script dashboard_data.sql`. Ce fichier contient :

- **CA par jour** (14 derniers jours) — via `DT_DAILY_REVENUE`
- **Top 10 produits** — via `DT_TOP_PRODUCTS`
- **Dernières commandes traitées** — jointure `STG_ORDERS` / `STG_CUSTOMERS`
- **KPIs** : nombre de commandes du jour, panier moyen


## Suspension des ressources

Le fichier `script suspension_ressources.sql` permet de **suspendre les Tasks et Dynamic Tables** en fin de session afin d'éviter toute consommation de crédits inutile :

- Tasks : `TSK_ROOT_PIPELINE`, `TSK_LOAD_STG_ORDER_ITEMS`, `TSK_LOAD_STG_WEB_EVENTS`, `TSK_LOAD_STG_ORDERS`
- Dynamic Tables : `DT_CUSTOMER_COHORTS`, `DT_DAILY_REVENUE`, `DT_TOP_PRODUCTS`

## Pipeline automatisé — test avec le lot J2

Pour démontrer le fonctionnement du pipeline CDC :

1. S'assurer que les Streams et Tasks sont actifs (script 03 exécuté)
2. Uploader les fichiers J2 (`orders_j2.csv`, `order_items_j2.csv`, `web_events_j2.json`) sur le stage
3. Charger les fichiers dans les tables RAW via `COPY INTO`
4. Observer les Streams se remplir → les Tasks se déclenchent automatiquement → STAGING et MARTS se mettent à jour

## Time Travel — procédure de recovery

En cas de suppression accidentelle de données :

```sql
-- Restaurer des lignes supprimées dans les 60 dernières secondes
INSERT INTO SHOPFLOW_DB.RAW.ORDERS
SELECT * FROM SHOPFLOW_DB.RAW.ORDERS AT (OFFSET => -60)
WHERE order_id NOT IN (SELECT order_id FROM SHOPFLOW_DB.RAW.ORDERS);
```

Pour une table entièrement supprimée :

```sql
UNDROP TABLE SHOPFLOW_DB.RAW.ORDERS;
```

## Points de vigilance

- Les warehouses sont configurés avec `AUTO_SUSPEND = 60s` pour limiter les coûts
- Penser à exécuter `script suspension_ressources.sql` en fin de travail
- Le script 02 est conservé à titre documentaire uniquement — ne pas l'exécuter
- Les fichiers du lot J2 ne doivent être chargés qu'après la mise en place des Streams/Tasks (script 03)

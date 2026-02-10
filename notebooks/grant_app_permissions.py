# Databricks notebook source

# MAGIC %md
# MAGIC # GenieRx: Grant App Permissions
# MAGIC
# MAGIC This notebook grants the GenieRx Databricks App's **service principal** access to the
# MAGIC resources it needs. Run through each cell in order after deploying the app.
# MAGIC
# MAGIC ### Resources configured by this notebook
# MAGIC
# MAGIC | Resource | Permission | Required For |
# MAGIC |----------|-----------|--------------|
# MAGIC | LLM Serving Endpoint | **Can Query** | Analyze + Optimize |
# MAGIC | SQL Warehouse | **Can Use** | Optimize mode |
# MAGIC | Genie Space(s) | **Can Edit** | Analyze + Optimize |
# MAGIC | Unity Catalog / Schema / Tables | **USE CATALOG**, **USE SCHEMA**, **SELECT** | Optimize mode |
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC 1. The GenieRx app has been deployed via **Compute > Apps**.
# MAGIC 2. You have **admin or manage** permissions on the resources listed above.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Configuration
# MAGIC
# MAGIC Fill in the widgets at the top of the notebook, then run each subsequent cell.

# COMMAND ----------

dbutils.widgets.text("app_name", "", "1. App Name (e.g. genie-space-analyzer)")
dbutils.widgets.text("serving_endpoint", "databricks-claude-sonnet-4-5", "2. LLM Serving Endpoint")
dbutils.widgets.text("sql_warehouse_id", "", "3. SQL Warehouse ID (Optimize mode)")
dbutils.widgets.text("genie_space_ids", "", "4. Genie Space IDs (comma-separated)")
dbutils.widgets.text("catalog_name", "", "5. Unity Catalog Name (Optimize mode)")
dbutils.widgets.text("schema_name", "", "6. Schema Name (Optimize mode)")
dbutils.widgets.text("table_names", "", "7. Table Names (comma-separated, or leave blank for all tables in schema)")

# COMMAND ----------

# Collect widget values
APP_NAME = dbutils.widgets.get("app_name").strip()
SERVING_ENDPOINT = dbutils.widgets.get("serving_endpoint").strip()
SQL_WAREHOUSE_ID = dbutils.widgets.get("sql_warehouse_id").strip()
GENIE_SPACE_IDS = [s.strip() for s in dbutils.widgets.get("genie_space_ids").split(",") if s.strip()]
CATALOG_NAME = dbutils.widgets.get("catalog_name").strip()
SCHEMA_NAME = dbutils.widgets.get("schema_name").strip()
TABLE_NAMES = [t.strip() for t in dbutils.widgets.get("table_names").split(",") if t.strip()]

assert APP_NAME, "App Name is required. Enter it in the widget above."
print(f"App Name:          {APP_NAME}")
print(f"Serving Endpoint:  {SERVING_ENDPOINT or '(skipped)'}")
print(f"SQL Warehouse ID:  {SQL_WAREHOUSE_ID or '(skipped)'}")
print(f"Genie Space IDs:   {GENIE_SPACE_IDS or '(skipped)'}")
print(f"Catalog:           {CATALOG_NAME or '(skipped)'}")
print(f"Schema:            {SCHEMA_NAME or '(skipped)'}")
print(f"Tables:            {TABLE_NAMES or '(all tables in schema)' if CATALOG_NAME else '(skipped)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Look Up the App Service Principal

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# List Databricks Apps and find the matching one
app_info = w.apps.get(APP_NAME)
sp_id = app_info.service_principal_id
assert sp_id, f"Could not find service principal for app '{APP_NAME}'. Ensure the app is deployed."

# Resolve the service principal name (used by permission and GRANT APIs)
sp = w.service_principals.get(sp_id)
SP_NAME = sp.display_name
SP_APP_ID = sp.application_id

print(f"Service Principal ID:   {sp_id}")
print(f"Service Principal Name: {SP_NAME}")
print(f"Application ID:         {SP_APP_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Grant LLM Serving Endpoint Permission
# MAGIC
# MAGIC The app calls the LLM endpoint to analyze Genie Space configurations.
# MAGIC This grants **Can Query** on the serving endpoint.

# COMMAND ----------

if SERVING_ENDPOINT:
    from databricks.sdk.service.serving import ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel

    endpoint = w.serving_endpoints.get(SERVING_ENDPOINT)
    endpoint_id = endpoint.id

    w.serving_endpoints.set_permissions(
        serving_endpoint_id=endpoint_id,
        access_control_list=[
            ServingEndpointAccessControlRequest(
                service_principal_name=SP_NAME,
                permission_level=ServingEndpointPermissionLevel.CAN_QUERY,
            )
        ],
    )
    print(f"Granted CAN_QUERY on serving endpoint '{SERVING_ENDPOINT}' to {SP_NAME}")
else:
    print("Skipped - no serving endpoint specified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Grant SQL Warehouse Permission
# MAGIC
# MAGIC Required for **Optimize mode** to execute benchmark SQL queries.
# MAGIC This grants **Can Use** on the SQL warehouse.

# COMMAND ----------

if SQL_WAREHOUSE_ID:
    from databricks.sdk.service.sql import SetRequest, ObjectPermissions, AccessControl, PermissionLevel

    # The permissions API path for SQL warehouses
    w.api_client.do(
        method="PATCH",
        path=f"/api/2.0/permissions/sql/warehouses/{SQL_WAREHOUSE_ID}",
        body={
            "access_control_list": [
                {
                    "service_principal_name": SP_NAME,
                    "permission_level": "CAN_USE",
                }
            ]
        },
    )
    print(f"Granted CAN_USE on SQL warehouse '{SQL_WAREHOUSE_ID}' to {SP_NAME}")
else:
    print("Skipped - no SQL warehouse ID specified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Grant Genie Space Permissions
# MAGIC
# MAGIC The app needs **Can Edit** on each Genie Space it will analyze or optimize.

# COMMAND ----------

if GENIE_SPACE_IDS:
    for space_id in GENIE_SPACE_IDS:
        try:
            w.api_client.do(
                method="PATCH",
                path=f"/api/2.0/permissions/genie-spaces/{space_id}",
                body={
                    "access_control_list": [
                        {
                            "service_principal_name": SP_NAME,
                            "permission_level": "CAN_EDIT",
                        }
                    ]
                },
            )
            print(f"Granted CAN_EDIT on Genie Space '{space_id}' to {SP_NAME}")
        except Exception as e:
            print(f"WARNING: Could not set permissions on Genie Space '{space_id}': {e}")
            print(f"  -> Open the Genie Space settings in the UI and add '{SP_NAME}' with 'Can Edit' manually.")
else:
    print("Skipped - no Genie Space IDs specified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Grant Unity Catalog Permissions
# MAGIC
# MAGIC Required for **Optimize mode** so the app can execute SQL on the tables your
# MAGIC Genie Space references. This grants **USE CATALOG**, **USE SCHEMA**, and
# MAGIC **SELECT** via SQL.

# COMMAND ----------

if CATALOG_NAME and SCHEMA_NAME:
    grants_executed = []

    # USE CATALOG
    stmt = f"GRANT USE CATALOG ON CATALOG `{CATALOG_NAME}` TO `{SP_NAME}`"
    spark.sql(stmt)
    grants_executed.append(stmt)

    # USE SCHEMA
    stmt = f"GRANT USE SCHEMA ON SCHEMA `{CATALOG_NAME}`.`{SCHEMA_NAME}` TO `{SP_NAME}`"
    spark.sql(stmt)
    grants_executed.append(stmt)

    # SELECT on tables
    if TABLE_NAMES:
        for table in TABLE_NAMES:
            stmt = f"GRANT SELECT ON TABLE `{CATALOG_NAME}`.`{SCHEMA_NAME}`.`{table}` TO `{SP_NAME}`"
            spark.sql(stmt)
            grants_executed.append(stmt)
    else:
        stmt = f"GRANT SELECT ON SCHEMA `{CATALOG_NAME}`.`{SCHEMA_NAME}` TO `{SP_NAME}`"
        spark.sql(stmt)
        grants_executed.append(stmt)

    print("Executed grants:")
    for g in grants_executed:
        print(f"  {g}")
elif CATALOG_NAME or SCHEMA_NAME:
    print("Both catalog and schema are required for Unity Catalog grants. Skipped.")
else:
    print("Skipped - no catalog/schema specified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Run the cell below to verify all permissions were applied.

# COMMAND ----------

print("=" * 60)
print("  GenieRx Permission Setup Complete")
print("=" * 60)
print()
print(f"  App:                {APP_NAME}")
print(f"  Service Principal:  {SP_NAME}")
print()

checks = {
    "LLM Serving Endpoint": bool(SERVING_ENDPOINT),
    "SQL Warehouse": bool(SQL_WAREHOUSE_ID),
    "Genie Space(s)": bool(GENIE_SPACE_IDS),
    "Unity Catalog Grants": bool(CATALOG_NAME and SCHEMA_NAME),
}

for resource, granted in checks.items():
    status = "GRANTED" if granted else "SKIPPED"
    print(f"  [{status:^8}]  {resource}")

skipped = [r for r, g in checks.items() if not g]
if skipped:
    print()
    print("  Skipped resources can be configured later by re-running")
    print("  this notebook with the appropriate widget values.")

print()
print("  Next steps:")
print("    1. Open the app and test with a Genie Space ID.")
print("    2. If you see 403 errors, verify the permissions above.")
print("    3. Remember to update app.yaml with SQL_WAREHOUSE_ID")
print("       and GENIE_TARGET_DIRECTORY if using Optimize mode.")
print()

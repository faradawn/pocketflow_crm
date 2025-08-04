CRM Backfill for Talbots 

Problem: Backfilling 30 files is time consuming. must happen day by day. The process is two step: CRM ingestion (with Dagster) and then identity resolution (with dbt code). If any of the two step fails, we need to fix the error and re-execute that step. 

Solution: create a workflow to execute CRM dagster (scrub files) → fix error with sql → run id res → fix error. With AI agent. 



Step 1: 

Node 1.1 Run CRM with dagster 

# Go to CRM dagster folder and execute dagster CRM ingestion with that date 


cd /Users/faradawn.yang/CS/map_crm/crm-dagster/
source /Users/faradawn.yang/CS/env_crm/bin/activate
dagster job execute -m crm_dagster  -j talbots_crm_ingest --tags '{"dagster/partition":"2025-07-16"}'



If Node 1.1 fails with message “Failure in test unique_talbots_raw_extraction_conversion_unique_key”, 

execute Node 1.2 (crm sql fix), which deletes the duplicate unique key.

cd /Users/faradawn.yang/CS/map_crm/

# 1) make a dated copy of the template
cp fix_unique_talbots_raw_extraction_conversion_unique_key.sql fix_unique_talbots_raw_extraction_conversion_unique_key_2025-07-16.sql

# 2) replace PLACEHOLDER_DATE with 2025-07-16 in that copy
sed -E -i '' 's/PLACEHOLDER_DATE/2025-07-16/g' fix_unique_talbots_raw_extraction_conversion_unique_key_2025-07-16.sql

# 3) run the script in BigQuery
bq query --project_id=talbots-og-map-dev --location=US --nouse_legacy_sql < fix_unique_talbots_raw_extraction_conversion_unique_key_2025-07-16.sql



If node 1.2 returns succss, that is contains the message ERROR=0. 

Then, go to node 1.3, run big crm dbt locally with the correct date

# Go back to CRM dbt folder
cd /Users/faradawn.yang/CS/map_crm/dbt
dbt build --target=oauth --profiles-dir=. --vars '{"client": "talbots", "project_id": "talbots-og-map-dev", "partition_key": "2025-07-16"}'



If node 1.3 succeeds, we go to Node 2.1 (id res start node).



If node 1.2 fails, that is, its output contains the “Error=xxx”. Stop the entire flow.



Step 2: Identity resolution



Node 2.1 (id res). Execute the identity resolution dbt. 

cd /Users/faradawn.yang/CS/map_identity_resolution/identity_resolution
source /Users/faradawn.yang/CS/env_id_res/bin/activate

# execute id res. The refresh date is a placeholder.
dbt build --target=oauth --vars '{"programmatic_client": "talbots", "project_id": "talbots-og-map-dev", "refresh_date": "2020-01-01"}'



If Node 2.1 success, that is, “ERROR=0”. Then, kick off the entire flow again with the next date. That is, go to Node 1.1 (CRM) and execute with the next date. 



If fails, go to , fix the error based on the failure. 

If failure is conversion_id_not_present, goto Node 2.2 (fix conversion_id_present), which execute this sql

bq query --project_id=talbots-og-map-dev --location=US --nouse_legacy_sql < fix_conversion_id_present.sql

If failture is other, stop the flow and throw a meesage 



If the fix of Node 2.2 solved the problem, go back to Node 2.1 and reexecute id res again.  

If Node 2.1 succeeds, echo “date xxx ingestion all done”, we finished 1 day of ingestions. We can go back to Node 1.1 to do CRM for the next day. 

If Node 2.1 fails, go to the failure connector and choose a fix based on the failure again. 


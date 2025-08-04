from pocketflow import Flow
from nodes import (
    RunCrmDagsterJob,
    FixDuplicateUniqueKeySql,
    RunCrmDbtBuild,
    FailedNode,
    IdRes,
    FixIdRes
)


def create_crm_ingestion_flow():
    """Return a Flow that executes Step-1 of the Talbots CRM backfill."""
    dagster_node = RunCrmDagsterJob()
    fix_node = FixDuplicateUniqueKeySql()
    dbt_node = RunCrmDbtBuild()

    # Wiring – see README / design.
    dagster_node - "dagster_ok" >> dbt_node
    dagster_node - "dagster_dup_key" >> fix_node
    fix_node - "fix_ok" >> dbt_node

    return Flow(start=dagster_node)


if __name__ == "__main__":
    dagster_node = RunCrmDagsterJob()
    fix_node = FixDuplicateUniqueKeySql()
    dbt_node = RunCrmDbtBuild()
    failed_node = FailedNode()
    id_res = IdRes()
    fix_idres = FixIdRes()


    fix_idres - "fix_ok" >> id_res

    flow = Flow(start=fix_idres)

    shared_store = {"date": "2025-07-16"}
    flow.run(shared_store)
    print("Flow finished. Shared store:", shared_store) 
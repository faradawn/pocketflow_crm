import subprocess
from pocketflow import Node



def run_shell(command: str, *, stream: bool = True):
    print("[run_shell] command: ", command)
    print("")
    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        executable="/bin/bash",
    )

    out_lines: list[str] = []
    # Read line-by-line to avoid buffering issues
    assert proc.stdout is not None  # for mypy/static checkers
    for line in proc.stdout:
        print(line, end="")    
        out_lines.append(line)

    proc.wait()
    output = "".join(out_lines)
    print("")
    print("[run_shell] finished", "return code", proc.returncode)
    print("")
    return proc.returncode, output, ""  # stderr merged into output


class RunCrmDagsterJob(Node):
    """Node 1.1 – execute Dagster CRM ingestion for a given date."""

    def prep(self, shared):
        date = shared["date"]  # Expect YYYY-MM-DD
        cmd = (
            "cd /Users/faradawn.yang/CS/map_crm/crm-dagster && "
            "deactivate && "
            "source /Users/faradawn.yang/CS/env_crm/bin/activate && "
            "dagster job execute -m crm_dagster -j talbots_crm_ingest "
            f"--tags '{{\"dagster/partition\":\"{date}\"}}'"
        )
        return cmd

    def exec(self, cmd):
        print("New run")
        return ("", "", "")
        return run_shell(cmd, stream=True)

    def post(self, shared, prep_res, exec_res):
        code, stdout, stderr = exec_res
        output = (stdout or "") + (stderr or "")
        print("Output of CRM Dagster code", code, "stdout", stdout)
        if code == 0:
            return "dagster_ok"
        if "unique_talbots_raw_extraction_conversion_unique_key" in output:
            return "dagster_dup_key"
        return "dagster_fail"


class FixDuplicateUniqueKeySql(Node):
    """Node 1.2 – run SQL to delete duplicate unique keys when Dagster fails."""

    def prep(self, shared):
        date = shared["date"]
        commands = [
            "cd /Users/faradawn.yang/CS/map_crm/",
            f"cp fix_unique_talbots_raw_extraction_conversion_unique_key.sql "
            f"fix_unique_talbots_raw_extraction_conversion_unique_key_{date}.sql",
            f"sed -E -i '' 's/PLACEHOLDER_DATE/{date}/g' "
            f"fix_unique_talbots_raw_extraction_conversion_unique_key_{date}.sql",
            f"bq query --project_id=talbots-og-map-dev --location=US --nouse_legacy_sql --verbosity=3 < "
            f"fix_unique_talbots_raw_extraction_conversion_unique_key_{date}.sql",
        ]
        return " && ".join(commands)

    def exec(self, cmd):
        print("=== Fix CRM SQL cmd", cmd)
        return run_shell(cmd, stream=True)

    def post(self, shared, prep_res, exec_res):
        retcode, stdout, stderr = exec_res
        output = (stdout or "") + (stderr or "")
        if retcode == 0:
            print("=== post fix: return code 0 fix ok!!!")
            return "fix_ok"
        else:
            print("=== post fix: fix fail!!!", "retcode", retcode, "output", output)
            return "fix_fail"

class RunCrmDbtBuild(Node):
    """Node 1.3 – run dbt build for CRM after Dagster + optional fix succeed."""

    def prep(self, shared):
        date = shared["date"]
        cmd = (
            "cd /Users/faradawn.yang/CS/map_crm/dbt && "
            "source /Users/faradawn.yang/CS/env_crm/bin/activate && "
            "dbt build --target=oauth --profiles-dir=. "
            f"--vars '{{\"client\": \"talbots\", \"project_id\": \"talbots-og-map-dev\", "
            f"\"partition_key\": \"{date}\"}}'"
        )
        print("===  Node 1.3 cmd", cmd)
        return cmd

    def exec(self, cmd):
        return run_shell(cmd)

    def post(self, shared, prep_res, exec_res):
        code, stdout, stderr = exec_res
        if code == 0:
            return "crm_done"
        return "crm_fail" 

class IdRes(Node):
    def prep(self, shared):
        cmd = (
            "cd /Users/faradawn.yang/CS/map_identity_resolution/identity_resolution && "
            "source /Users/faradawn.yang/CS/env_id_res/bin/activate && "
            "dbt build --target=oauth --vars '{\"programmatic_client\": \"talbots\", \"project_id\": \"talbots-og-map-dev\", \"refresh_date\": \"2020-01-01\"}'"
        )
        print("=== dbt command", cmd)
        return cmd

    def exec(self, cmd):
        return run_shell(cmd, stream=True)

    def post(self, shared, prep_res, exec_res):
        code, stdout, stderr = exec_res

        output = (stdout or "") + (stderr or "")
        print("id res stdout", stdout)
        print("id res code", code)
        if code == 0:
            print("=== id res done, ok")
            return "idres_ok"
        if code == 1 and "Failure in test conversion_ids_are_present" in output:
            print("=== id res done, conv id not present")
            return "idres_conv_fix"
        
        return "idres_fail with other error"

class FixIdRes(Node):
    def prep(self, shared):
        return ""
    def exec(self, cmd):
        cmd = (
            "bq query --project_id=talbots-og-map-dev --location=US --nouse_legacy_sql --verbosity=2 < /Users/faradawn.yang/CS/pocketflow_crm/fix_conversion_ids_are_present.sql"
        )
        return run_shell(cmd)
    def post(self, shared, prep_res, exec_res):
        code, stdout, stderr = exec_res

        output = (stdout or "") + (stderr or "")
        if code == 0:
            print("=== fix ok")
            return "fix_ok"
        else:
            print("=== id res fix failed")
            return "fix_failed"
        
class FailedNode(Node):
    def prep(self, shared):
        return ""
    def exec(self, cmd):
        print("=== FailedNode")
    def post(self, shared, prep_res, exec_res):
        pass
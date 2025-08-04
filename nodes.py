import subprocess
import os
import datetime
from pocketflow import Node, Flow

LOG_FILE = "/Users/faradawn.yang/CS/pocketflow_crm/crm_log.txt"

def run_shell(command: str, *, stream: bool = True):
    print("=== Executing command: ", command)
    
    detailed_log_file = "/Users/faradawn.yang/CS/pocketflow_crm/crm_detailed_log.txt"
    
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
        if stream:
            print(line, end="")    
        out_lines.append(line)

    proc.wait()
    output = "".join(out_lines)
    
    # Write to detailed log file
    with open(detailed_log_file, "a", encoding="utf-8") as log_f:
        log_f.write(f"\n=== Command executed at {datetime.datetime.now()}: {command}\n")
        log_f.write(f"Return code: {proc.returncode}\n")
        log_f.write("Output:\n")
        log_f.write(output)
        log_f.write("\n" + "="*80 + "\n")
    
    print("")
    print("=== command finished", "return code", proc.returncode)
    print("")
    return proc.returncode, output, ""  # stderr merged into output


class CRMDagster(Node):
    """Node 1.1 – execute Dagster CRM ingestion for a given date."""

    def prep(self, shared):
        # Load last date from log and increment by 1 day
        with open(LOG_FILE, 'r') as f:
            last_date = f.readlines()[-1].strip()
        
        from datetime import datetime, timedelta
        date_obj = datetime.strptime(last_date, '%Y-%m-%d')
        next_date = date_obj + timedelta(days=1)
        shared['date'] = next_date.strftime('%Y-%m-%d')

        print("=== RunCrmDagsterJob executing date", shared["date"])

        cmd = (
            "cd /Users/faradawn.yang/CS/map_crm/crm-dagster && "
            "source /Users/faradawn.yang/CS/env_crm/bin/activate && "
            "dagster job execute -m crm_dagster -j talbots_crm_ingest "
            f"--tags '{{\"dagster/partition\":\"{shared["date"]}\"}}' "
        )
        return cmd

    def exec(self, cmd):
        return run_shell(cmd, stream=False)

    def post(self, shared, prep_res, exec_res):
        code, stdout, stderr = exec_res
        output = (stdout or "") + (stderr or "")
        print("Output of CRM Dagster code", code, "stdout", stdout)
        if code == 0:
            return "ok"
        elif code == 1 and "Failure in test unique_talbots_raw_extraction_conversion_unique_key" in output:
            return "error_conversion_unique_key"
        else:
            return "error_others"


class FixCRM(Node):
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
            return "ok"
        else:
            print("=== post fix: fix fail!!!", "retcode", retcode, "output", output)
            return "error"

class CRMDbt(Node):
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
            return "ok"
        else:
            return "error" 

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
        return 0, "", ""
        return run_shell(cmd, stream=True)

    def post(self, shared, prep_res, exec_res):
        code, stdout, stderr = exec_res

        output = (stdout or "") + (stderr or "")
        print("id res stdout", stdout)
        print("id res code", code)
        if code == 0:
            print("=== id res done, writing to log")
            if not os.path.exists(LOG_FILE):
                with open(LOG_FILE, "w", encoding="utf-8") as fh:
                    pass
            with open(LOG_FILE, "a", encoding="utf-8") as fh:
                fh.write(f"{shared['date']}\n")
            return "ok"
        if code == 1 and "Failure in test conversion_ids_are_present" in output:
            print("=== id res done, conv id not present")
            return "error_conversion_ids_are_present"
        
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
            return "ok"
        else:
            print("=== id res fix failed")
            return "error"
        
class FailedNode(Node):
    def prep(self, shared):
        return ""
    def exec(self, cmd):
        print("=== FailedNode")
    def post(self, shared, prep_res, exec_res):
        pass

if __name__ == "__main__":
    dagster_crm = CRMDagster()
    fix_crm = FixCRM()
    crm_dbt = CRMDbt()

    id_res = IdRes()
    fix_idres = FixIdRes()

    failed_node = FailedNode()
    
    
    dagster_crm - "ok" >> id_res
    dagster_crm - "error_conversion_unique_key" >> fix_crm
    dagster_crm - "error_others" >> failed_node

    fix_crm - "ok" >> crm_dbt
    fix_crm - "error" >> failed_node

    crm_dbt - "ok" >> id_res
    fix_crm - "error" >> failed_node

    id_res - "ok" >> dagster_crm
    id_res - "error_conversion_ids_are_present" >> fix_idres

    fix_idres - "ok" >> id_res
    fix_idres - "error" >> failed_node


    flow = Flow(start=dagster_crm)
    shared = {}
    shared["date"] = "null"
    flow.run(shared)
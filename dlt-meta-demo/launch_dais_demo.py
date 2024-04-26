"""DLT-META DAIS DEMO script."""
import uuid
import webbrowser
from databricks.sdk.service import jobs
from dlt-meta-demo.demo.helpers.install import WorkspaceInstaller
from dlt-meta-demo.demo.helpers.dlt_meta_runner_helpers import (
    DLTMETARunner,
    DLTMetaRunnerConf,
    cloud_node_type_id_dict,
    get_workspace_api_client,
    process_arguments
)

def main():
    """Entry method to run integration tests."""
    args = process_arguments()

    api_client = get_api_client()
    username = api_client.perform_query("GET", "/preview/scim/v2/Me").get("userName")
    run_id = uuid.uuid4().hex
    dbfs_tmp_path = f"{args.__dict__['dbfs_path']}/{run_id}"
    database = f"dais_dlt_meta_{run_id}"
    int_tests = "demo/"
    runners_nb_path = f"/Users/{username}/dais_dlt_meta/{run_id}"
    runners_full_local_path = 'demo/dais_dlt_meta_runners.dbc'

    dbfs_service = DbfsService(api_client)
    jobs_service = JobsService(api_client)
    workspace_service = WorkspaceService(api_client)
    pipeline_service = DeltaPipelinesService(api_client)

    try:
        create_onboarding(dbfs_tmp_path, run_id)

        DbfsApi(api_client).cp(True, True, int_tests, dbfs_tmp_path + "/")
        fp = open(runners_full_local_path, "rb")
        workspace_service.mkdirs(path=runners_nb_path)
        workspace_service.import_workspace(path=f"{runners_nb_path}/runners", format="DBC",
                                           content=base64.encodebytes(fp.read()).decode('utf-8'))
        bronze_pipeline_id = create_dlt_meta_pipeline(
            pipeline_service, runners_nb_path, run_id, configuration={
                "layer": "bronze",
                "bronze.group": "A1",
                "bronze.dataflowspecTable": f"{database}.bronze_dataflowspec_cdc"
            }
        )

        cloud_node_type_id_dict = {"aws": "i3.xlarge",
                                   "azure": "Standard_D3_v2",
                                   "gcp": "n1-highmem-4"
                                   }
        job_spec_dict = {"run_id": run_id,
                         "dbfs_tmp_path": dbfs_tmp_path,
                         "runners_nb_path": runners_nb_path,
                         "database": database,
                         "env": "prod",
                         "bronze_pipeline_id": bronze_pipeline_id,
                         "node_type_id": cloud_node_type_id_dict[args.__dict__['cloud_provider_name']],
                         "dbr_version": args.__dict__['dbr_version']
                         }

        silver_pipeline_id = create_dlt_meta_pipeline(
            pipeline_service, runners_nb_path, run_id, configuration={
                "layer": "silver",
                "silver.group": "A1",
                "silver.dataflowspecTable": f"{database}.silver_dataflowspec_cdc"
            }
        )
        job_spec_dict["silver_pipeline_id"] = silver_pipeline_id

        job_spec = create_workflow_spec(job_spec_dict)
        
        job_submit_runner = JobSubmitRunner(jobs_service, job_spec)

        job_run_info = job_submit_runner.submit()
        print(f"Run URL {job_run_info['run_id']}")

        job_submit_runner.monitor(job_run_info['run_id'])

    except Exception as e:
        print(e)
    finally:
        pipeline_service.delete(bronze_pipeline_id)
        pipeline_service.delete(silver_pipeline_id)
        dbfs_service.delete(dbfs_tmp_path, True)
        workspace_service.delete(runners_nb_path, True)
        try:
            os.remove("conf/onboarding.json")
        except Exception as e:
            print(e)


def process_arguments():
    """Process command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--cloud_provider_name",
                        help="provide cloud provider name. Supported values are aws , azure , gcp")
    parser.add_argument("--dbr_version", help="Provide databricks runtime spark version e.g 11.3.x-scala2.12")
    parser.add_argument("--dbfs_path",
                        help="Provide databricks workspace dbfs path where you want run integration tests \
                        e.g --dbfs_path=dbfs:/tmp/DLT-META/")
    args = parser.parse_args()
    mandatory_args = ["cloud_provider_name", "dbr_version", "dbfs_path"]
    check_mandatory_arg(args, mandatory_args)

    supported_cloud_providers = ["aws", "azure", "gcp"]

    cloud_provider_name = args.__getattribute__("cloud_provider_name")
    if cloud_provider_name.lower() not in supported_cloud_providers:
        raise Exception("Invalid value for --cloud_provider_name! Supported values are aws, azure, gcp")

    print(f"Parsing argument complete. args={args}")
    return args


def check_mandatory_arg(args, mandatory_args):
    """Check mandatory argument present."""
    for mand_arg in mandatory_args:
        if args.__dict__[f'{mand_arg}'] is None:
            raise Exception(f"Please provide '--{mand_arg}'")


if __name__ == "__main__":
    main()

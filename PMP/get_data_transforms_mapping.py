from mp_client import Client, Project
import json
import pandas as pd
import time
from typing import List


def get_datasource_inside_pipeline(client: Client, pipeline_pk: str) -> List[str]:
    """

    Parameters
    ----------
    client
    pipeline_pk

    Returns
    -------

    """
    api_client = client._api_client
    session = api_client.session
    resource_type = "pipeline"
    url = api_client.base_url + "/{}/{}/flow".format(resource_type, pipeline_pk)
    pipeline_dict = session.get(url).json()
    pipeline_ds = [ds for ds in pipeline_dict if ds["classname"] == "DataSource"]

    return pipeline_ds

def get_datatransform_inside_pipeline(client: Client, pipeline_pk: str) -> List[str]:
    """

    Parameters
    ----------
    client
    pipeline_pk

    Returns
    -------

    """
    api_client = client._api_client
    session = api_client.session
    resource_type = "pipeline"
    url = api_client.base_url + "/{}/{}/flow".format(resource_type, pipeline_pk)
    pipeline_dict = session.get(url).json()
    pipeline_dts = [dt for dt in pipeline_dict if dt["classname"] == "Macro"]

    return pipeline_dts

def get_datasource(client: Client, pk_datasource: str) -> json:
    """

    Parameters
    ----------
    client
    pk_datasource

    Returns
    -------

    """
    api_client = client._api_client
    # session = api_client.session
    response = api_client.get_datasource(pk_datasource)

    return response

def get_projects_by_name_fork(client: Client, pattern: str) -> List[Project]:
    """

    Parameters
    ----------
    client
    pattern

    Returns
    -------

    """
    result = []
    all_projects = client.get_projects()
    for project in all_projects:
        if pattern == project.name:
            project.set_client(client._api_client)
            result.append(project)

    if not result:
        print(f"Cannot find any project called {pattern}")
    return result

def get_connector(client: Client, pk) -> dict:
    """

    Parameters
    ----------
    client
    pk

    Returns
    -------

    """
    resource_type = "connector"
    api_client = client._api_client
    session = api_client.session
    url = api_client.base_url + "/{}/{}/".format(resource_type, pk)
    result = session.get(url)
    if result.ok:
        connector_dict = result.json()
        return connector_dict
    else:
        print(f"Error: {result}")


if __name__ == "__main__":
    start_time = time.time()

    # read creds
    with open('./creds.json') as f:
        creds_dict = json.load(f)

    base_url = creds_dict["url"]
    user = creds_dict["user"]
    password = creds_dict["password"]
    upstream_mapping = True

    # Authenticating
    client = Client.from_credentials(url=base_url, user=user, password=password)

    output_name = "bper_planning_dt_and_ds_map.xlsx"

    # params
    projects_names = [
        # "PREPROCESSING_APPLICATION_SIGHT_DEPOSIT_VOLUMES_SPARK",
        # "PREPROCESSING_ESTIMATION_PREPAYMENT_SPARK",
        # "PREPROCESSING_ESTIMATION_SIGHT_DEPOSIT_RATES_SPARK",
        # "PREPROCESSING_ESTIMATION_SIGHT_DEPOSIT_VOLUME_SPARK",
        # "PREPROCESSING_APPLICATION_PREPAYMENT_SPARK",
        # "SCENARIO_DATA_PREPARATION",
        # "SIGHT_DEPOSIT_STABLE_APPLICATION",
        # "BPER_PREPAYMENT_APPLICATION",
        # "SIGHT_DEPOSIT_DECAY_APPLICATION",
        # "SIGHT_DEPOSIT_STABLE_ESTIMATION",
        # "BPER_PREPAYMENT_ESTIMATION",  # NOT WORKING !! -- # TO DO:
        # "SIGHT_DEPOSIT_DECAY_ESTIMATION",
        # "BPER_RATES_MODEL_ESTIMATION"
        "BPER_PLANNING_PREPROCESSING_20240630",
        # "PREPROCESSING_ESTIMATION_SIGHT_DEPOSIT_RATES_SPARK",
        # "PREPROCESSING_ESTIMATION_SIGHT_DEPOSIT_VOLUME_SPARK",
        # "SIGHT_DEPOSIT_STABLE_ESTIMATION",
        # "SIGHT_DEPOSIT_DECAY_ESTIMATION",
        # "PREPROCESSING_APPLICATION_SIGHT_DEPOSIT_VOLUMES_SPARK",
        # "SIGHT_DEPOSIT_STABLE_APPLICATION",
        # "SIGHT_DEPOSIT_DECAY_APPLICATION",
        # "PREPROCESSING_ESTIMATION_PREPAYMENT_SPARK",
        # "BPER_PREPAYMENT_ESTIMATION",
        # "PREPROCESSING_APPLICATION_PREPAYMENT_SPARK",
        # "BPER_PREPAYMENT_APPLICATION"

    ]

    shorten_project_names = {
        "BPER_PLANNING_PREPROCESSING_20240630": "BPER_PLANNING_PREPROC"
        # "PREPROCESSING_APPLICATION_SIGHT_DEPOSIT_VOLUMES_SPARK": "PREPROC_APPLIC_S_D_VOLUMES",
        # "PREPROCESSING_ESTIMATION_PREPAYMENT_SPARK": "PREPROC_EST_PREPAYMENT",
        # "PREPROCESSING_ESTIMATION_SIGHT_DEPOSIT_RATES_SPARK": "PREPROC_EST_S_D_RATES",
        # "PREPROCESSING_ESTIMATION_SIGHT_DEPOSIT_VOLUME_SPARK": "PREPROC_ESTI_S_D_VOLUME",
        # "PREPROCESSING_APPLICATION_PREPAYMENT_SPARK": "PREPROC_APPLICATION_PREPAYMENT",
        # "SCENARIO_DATA_PREPARATION": "SCENARIO_DATA_PREPARATION",
        # "SIGHT_DEPOSIT_STABLE_APPLICATION": "S_D_STABLE_APPLICATION",
        # "BPER_PREPAYMENT_APPLICATION": "BPER_PREPAYMENT_APPLICATION",
        # "SIGHT_DEPOSIT_DECAY_APPLICATION": "S_D_DECAY_APPLICATION",
        # "SIGHT_DEPOSIT_STABLE_ESTIMATION": "S_D_STABLE_EST",
        # "BPER_PREPAYMENT_ESTIMATION": "BPER_PREPAYMENT_EST",
        # "SIGHT_DEPOSIT_DECAY_ESTIMATION": "S_D_DECAY_EST",
        # "BPER_RATES_MODEL_ESTIMATION": "BPER_RATES_MODEL_EST"
    }

    # get projects to process
    projects = [
        client.get_project("66e301c6afbdd454c0f1b651")
    ]

    dfs = {}
    n_prj = 0
    for project in projects:
        n_prj += 1
        print(f"prj_n:: {n_prj}")
        print(f"processing project: {project.name}")
        all_res = project.get_resources()
        pip_dict = {i.uid[:-6]: i.pk for i in all_res if i._cls == 'DataFlow'}

        data_transforms = {
            pipe_name: get_datatransform_inside_pipeline(client=client, pipeline_pk=pk) for pipe_name, pk in
            pip_dict.items()
        }

        dt_info = {}
        if upstream_mapping:
            print("upstream mapping: ")
            for pipe_name, pipeline_dts in data_transforms.items():
                data_ = []
                if pipe_name == "PREPROCESSING_DATA":
                    print(
                        f"processing upstream_mapping for pipeline: {pipe_name}, data transforms: {len(pipeline_dts)}"
                    )
                    for dt in pipeline_dts:
                        dt_name = dt["uid"][:-6]
                        output_ds_pk = dt["children"][0]["pk"] if dt["children"][0]["label"] == "output_dataset" else None
                        inputs_ds = client.get_data_transform(dt["pk"]).get_input()
                        data = {
                            dt_name: {
                                "input": inputs_ds,
                                "output": get_datasource(client=client, pk_datasource=output_ds_pk)
                            }
                        }
                        data_.append(data)

                        dt_info[pipe_name] = data_

        remote_ds_count = 0
        local_ds_count = 0
        if upstream_mapping:
            df = pd.DataFrame(
                columns=[
                    # "PIPELINE",
                    "DATA_TRANSFORM",
                    "INPUT_DATASETS",
                    # "INPUT_TYPE",
                    # "INPUT_CONNECTOR_NAME",
                    # "INPUT_FILE_NAME",
                    "OUTPUT_DATASET",
                    # "OUTPUT_TYPE",
                    # "OUTPUT_CONNECTOR_NAME",
                    # "OUTPUT_FILE_NAME",
                    # "TABLE_NAME",
                    # "STORAGE_ACCOUNT_NAME",
                    # "CONTAINER_NAME",
                    # "FOLDER_NAME"
                ]
            )

            for pipe_name, dt in dt_info.items():
                for data in dt:
                    for dt_name, dt_obj in data.items():
                        # input_remote_connector = dt_obj["input"]["connector"].get("remote_connector")
                        # input_filename = dt_obj["input"]["connector"].get("filename")
                        output_remote_connector = dt_obj["output"]["connector"].get("remote_connector")
                        # output_filename = dt_obj["output"]["connector"].get("filename")
                        # table_name = d["connector"].get("tablename")

                        if output_remote_connector:
                        #     connector_info = get_connector(client=client, pk=input_remote_connector)
                        #
                        #     # storage_account = connector_info.get("storage_account")
                        #     # container_name = connector_info.get("container_name")
                        #     # path = connector_info.get("path")
                        #     connector_name = connector_info.get("name")

                            df.loc[len(df)] = (
                                # pipe_name,
                                dt_name,
                                ", \n".join([i for i in [obj.uid[:-6] for obj in dt_obj["input"]["input_datasets"]]]),
                                dt_obj["output"]["ID"],
                                # "remote",
                                # connector_name,
                                # filename
                                # table_name,
                                # storage_account,
                                # container_name,
                                # path
                            )
                            remote_ds_count += 1

                        else:
                            df.loc[len(df)] = (
                                # pipe_name,
                                dt_name,
                                ", \n".join([i for i in [obj.uid[:-6] for obj in dt_obj["input"]["input_datasets"]]]),
                                dt_obj["output"]["ID"]
                                # "local",
                                # "",
                                # "",
                                # "",
                                # "",
                                # "",
                                # ""
                            )
                            local_ds_count += 1
                            # print("skipping local data sources...")
                            # print(f"{d['uid']}", [k for k in d["connector"].keys()])

                dfs[pipe_name] = df
                # fragmented operation (on disk).
                # df.to_csv(f"data/{shorten_project_names[project.name]}", sep=";", index=False)

        print(f"REMOTE data sources -> {remote_ds_count}")
        print(f"LOCAL data sources -> {local_ds_count}")
        print(f"TOTAL data sources: {local_ds_count + remote_ds_count}")
        print("__________________________________________________________________________________")

    with pd.ExcelWriter(output_name) as writer:
        print("Writing .xlsx file..")
        for pipe_name, df in dfs.items():
            df.to_excel(writer, sheet_name=pipe_name, index=False)

            workbook = writer.book
            worksheet = writer.sheets[pipe_name]

            cell_format = workbook.add_format({"text_wrap": True})
            worksheet.set_column("A:A", 20, cell_format)  # Adjust column width

    green = '\033[92m'
    reset = '\033[0m'
    print("__________________________________________________________________________________")
    print("\n")
    print(
        green + f"Success. in (%s seconds). "
        % round((time.time() - start_time), 2), f"\nFile '{output_name}', written." + reset
    )

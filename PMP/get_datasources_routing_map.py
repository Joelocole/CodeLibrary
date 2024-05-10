from mp_client import Client, Project
import json
import pandas as pd
import requests
from typing import List


def get_datasource_inside_pipeline(
        client: Client,
        pipeline_pk,
):
    api_client = client._api_client
    session = api_client.session
    resource_type = "pipeline"
    url = api_client.base_url + "/{}/{}/flow".format(resource_type, pipeline_pk)
    pipeline_dict = session.get(url).json()
    pipeline_ds = [ds for ds in pipeline_dict if ds["classname"] == "DataSource"]

    return pipeline_ds


def get_datasource(base_url: str, pk_datasource: str, api: str = "/api/v0/") -> json:
    api_client = client._api_client
    session = api_client.session
    response = session.get(base_url + api + "datasource/{}/".format(pk_datasource))

    try:
        response.raise_for_status()
    except requests.HTTPError as err:
        print(f"Error in retrieving datasource metadata: {err}")
        print(response.json())

    response = response.json()

    return response


def get_projects_by_name_fork(client, pattern: str) -> List[Project]:
    result = []
    all_projects = client.get_projects()
    for project in all_projects:
        if pattern == project.name:
            project.set_client(client._api_client)
            result.append(project)

    if not result:
        print(f"Cannot find any project called {pattern}")
    return result


def get_connector(base_url: str, pk) -> dict:
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
    # read creds
    with open('./creds.json') as f:
        creds_dict = json.load(f)

    base_url = creds_dict["url"]
    user = creds_dict["user"]
    password = creds_dict["password"]

    # Authenticating
    client = Client.from_credentials(url=base_url, user=user, password=password)

    # params
    projects_names = [
        "PREPROCESSING_APPLICATION_SIGHT_DEPOSIT_VOLUMES_SPARK",
        "PREPROCESSING_ESTIMATION_PREPAYMENT_SPARK",
        "PREPROCESSING_ESTIMATION_SIGHT_DEPOSIT_RATES_SPARK",
        "PREPROCESSING_ESTIMATION_SIGHT_DEPOSIT_VOLUME_SPARK",
        "PREPROCESSING_APPLICATION_PREPAYMENT_SPARK",
        "SCENARIO_DATA_PREPARATION",
        "SIGHT_DEPOSIT_STABLE_APPLICATION",
        "BPER_PREPAYMENT_APPLICATION",
        "SIGHT_DEPOSIT_DECAY_APPLICATION",
        "SIGHT_DEPOSIT_STABLE_ESTIMATION",
        "BPER_PREPAYMENT_ESTIMATION",  # NOT WORKING !! -- # TO DO:
        "SIGHT_DEPOSIT_DECAY_ESTIMATION",
        "BPER_RATES_MODEL_ESTIMATION"
    ]

    shorten_project_names = {
        "PREPROCESSING_APPLICATION_SIGHT_DEPOSIT_VOLUMES_SPARK": "PREPROC_APPLIC_S_D_VOLUMES",
        "PREPROCESSING_ESTIMATION_PREPAYMENT_SPARK": "PREPROC_EST_PREPAYMENT",
        "PREPROCESSING_ESTIMATION_SIGHT_DEPOSIT_RATES_SPARK": "PREPROC_EST_S_D_RATES",
        "PREPROCESSING_ESTIMATION_SIGHT_DEPOSIT_VOLUME_SPARK": "PREPROC_ESTI_S_D_VOLUME",
        "PREPROCESSING_APPLICATION_PREPAYMENT_SPARK": "PREPROC_APPLICATION_PREPAYMENT",
        "SCENARIO_DATA_PREPARATION": "SCENARIO_DATA_PREPARATION",
        "SIGHT_DEPOSIT_STABLE_APPLICATION": "S_D_STABLE_APPLICATION",
        "BPER_PREPAYMENT_APPLICATION": "BPER_PREPAYMENT_APPLICATION",
        "SIGHT_DEPOSIT_DECAY_APPLICATION": "S_D_DECAY_APPLICATION",
        "SIGHT_DEPOSIT_STABLE_ESTIMATION": "S_D_STABLE_EST",
        "BPER_PREPAYMENT_ESTIMATION": "BPER_PREPAYMENT_EST",
        "SIGHT_DEPOSIT_DECAY_ESTIMATION": "S_D_DECAY_EST",
        "BPER_RATES_MODEL_ESTIMATION": "BPER_RATES_MODEL_EST"
    }

    # get projects to process
    projects = [
        project for p in projects_names for project in get_projects_by_name_fork(client=client, pattern=p)
    ]

    dfs = {}
    for project in projects:
        print(f"processing project: {project.name}")
        all_res = project.get_resources()
        pip_dict = {i.uid[:-6]: i.pk for i in all_res if i._cls == 'DataFlow'}

        data_sources = {
            pipe_name: get_datasource_inside_pipeline(client=client, pipeline_pk=pk) for pipe_name, pk in
            pip_dict.items()
        }

        ds_info = {}
        for pipe_name, pipeline_ds in data_sources.items():
            print(f"processing pipeline: {pipe_name}, data sources: {len(pipeline_ds)}")
            data_ = []
            for ds in pipeline_ds:
                ds_name = ds["uid"][:-6]

                data = get_datasource(base_url=base_url, pk_datasource=ds["pk"])
                data_.append(data)

            ds_info[pipe_name] = data_

        df = pd.DataFrame(
            columns=[
                "PIPELINE",
                "DATASOURCE_NAME",
                "CONNECTOR_NAME",
                "FILE_NAME",
                "STORAGE_ACCOUNT_NAME",
                "CONTAINER_NAME",
                "FOLDER_NAME"
            ]
        )

        resource_count = 0
        local_resources = 0
        for pipe_name, ds in ds_info.items():
            for d in ds:
                remote_connector = d["connector"].get("remote_connector")
                filename = d["connector"].get("filename")

                if remote_connector:
                    connector_info = get_connector(base_url=base_url, pk=remote_connector)

                    storage_account = connector_info.get("storage_account")
                    container_name = connector_info.get("container_name")
                    path = connector_info.get("path")
                    connector_name = connector_info.get("name")

                    df.loc[len(df)] = (
                        pipe_name,
                        d["ID"],
                        connector_name,
                        filename,
                        storage_account,
                        container_name,
                        path
                    )
                    resource_count += 1

                else:
                    local_resources += 1
                    print("skipping local data sources...")
                    # print(f"{d['uid']}", [k for k in d["connector"].keys()])

            dfs[project.name] = df

            # fragmented operation (on disk).
            # df.to_csv(f"data/{shorten_project_names[project.name]}", sep=";", index=False)

        print(f"REMOTE data sources -> {resource_count}")
        print(f"skipped LOCAL data sources: {local_resources}")
        print(f"TOTAL data sources: {local_resources + resource_count}")
        print("____________________________________________________________________________")
        print("\n")

    with pd.ExcelWriter("bper_datasource_routing_map.xlsx") as writer:
        for table_name, df in dfs.items():
            df.to_excel(writer, sheet_name=shorten_project_names[table_name], index=False)

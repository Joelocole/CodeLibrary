from mp_client import Client
import json
import pandas as pd
import requests


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

    response = Session.get(base_url + api + "datasource/{}/".format(pk_datasource))

    try:
        response.raise_for_status()
    except requests.HTTPError as err:
        print(f"Error in retrieving datasource metadata: {err}")
        print(response.json())

    response = response.json()
    # pipeline_ds = [ds for ds in pipeline_dict if ds["classname"] == "DataSource"]

    return response


def get_blob_connector_info(base_url, connector_pk: str) -> dict:
    """
    Retrieve blob storage connector info from given connector_pk
    :return: Dict with connector info
    """

    url = base_url + f"/connector/{connector_pk}"
    response = requests.get(url)

    return response


if __name__ == "__main__":

    with open('./creds.json') as f:
        creds_dict = json.load(f)

    base_url = creds_dict["url"]
    user = creds_dict["user"]
    password = creds_dict["password"]

    # Authenticating
    client = Client.from_credentials(url=base_url, user=user, password=password)

    Session = client._api_client.session

    projects_names = [
        # "PREPROCESSING_APPLICATION_SIGHT_DEPOSIT_VOLUMES_SPARK",
        # "PREPROCESSING_ESTIMATION_PREPAYMENT_SPARK",
        # "PREPROCESSING_ESTIMATION_SIGHT_DEPOSIT_RATES_SPARK",
        # "PREPROCESSING_ESTIMATION_SIGHT_DEPOSIT_VOLUME_SPARK",
        "PREPROCESSING_APPLICATION_PREPAYMENT_SPARK",
        # "SCENARIO_DATA_PREPARATION",
        # "SIGHT_DEPOSIT_STABLE_APPLICATION",
        # "BPER_PREPAYMENT_APPLICATION",
        # "SIGHT_DEPOSIT_DECAY_APPLICATION",
        # "SIGHT_DEPOSIT_STABLE_ESTIMATION",
        # "BPER_PREPAYMENT_ESTIMATION",
        # "SIGHT_DEPOSIT_DECAY_ESTIMATION",
        # "BPER_RATES_MODEL_ESTIMATION"
    ]

    projects = [project for p in projects_names for project in client.get_projects_by_name(p)]

    # project = client.get_projects()
    # for p in project:
    #     if ("test" not in p.name.lower()) & ("[CONVALIDA]" not in p.name):
    #         print(p)
    # data_sources = []
    for project in projects:
        print(project.name)
        all_res = project.get_resources()
        pip_dict = {i.uid[:-6]: i.pk for i in all_res if i._cls == 'DataFlow'}

        data_sources = [
            get_datasource_inside_pipeline(client=client, pipeline_pk=pk) for pipe_name, pk in pip_dict.items()
        ]

        # for pipe_name, pk in pip_dict.items():
        #     ds = get_datasource_inside_pipeline(client=client, pipeline_pk=pk)
        #     data_sources.append(ds)

        # if len(pip_dict.keys()) > 1:
        #     for name, pk in pip_dict.items():
        #         ds = get_datasource_inside_pipeline(client, pk)
        #         data_sources.append(ds)
        # else:
        #     tmp_pk = [v for k, v in pip_dict.items()]
        #     ds = get_datasource_inside_pipeline(client, *tmp_pk)
        #     data_sources.append(ds)

        ds_info = []
        for pipeline_ds in data_sources:
            # print(pipeline_ds)
            for ds in pipeline_ds:
                ds_name = ds["uid"][:-6]

                data = get_datasource(base_url=base_url, pk_datasource=ds["pk"])
                ds_info.append(data)

        for d in ds_info:
            if ("remote_connector" in d["connector"].keys()) & (not d["connector"]["is_local"]):
                connector_info = get_blob_connector_info(base_url=base_url, connector_pk=d["connector"]["remote_connector"])
                print(connector_info)





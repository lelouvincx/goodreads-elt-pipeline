from dagster import IOManager, InputContext, OutputContext
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request

import io
import os
import pickle
import polars as pl
from typing import Union
from contextlib import contextmanager
from .minio_io_manager import connect_minio, make_bucket


@contextmanager
def gdrive_client(config):
    client_secret_file = config["client_secret_file"]
    pickle_file = config["pickle_file"]
    api_name = config["api_name"]
    api_version = config["api_version"]
    scopes = config["scopes"]

    cred = None

    if os.path.exists(pickle_file):
        with open(pickle_file, "rb") as token:
            cred = pickle.load(token)
    else:
        raise Exception(
            f"Pickle not exists from this, pickle_file: {pickle_file} and cred file: {cred}"
        )

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, scopes)
            cred = flow.run_local_server()

        with open(pickle_file, "wb") as token:
            pickle.dump(cred, token)

    try:
        service = build(api_name, api_version, credentials=cred)
        yield service
    except Exception as e:
        raise Exception("Error while creating gdrive client: {}".format(e))


class GDriveIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def download_files(
        self,
        context,
        dowid,
        dfilespath,
        folder=None,
    ):
        """
        Download files from gdrive with given id
        """

        with gdrive_client(self._config) as service:
            request = service.files().get_media(fileId=dowid)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                context.log.debug("Download %d%%." % int(status.progress() * 100))
            if folder:
                with io.open(folder + "/" + dfilespath, "wb") as f:
                    fh.seek(0)
                    f.write(fh.read())
            else:
                with io.open(dfilespath, "wb") as f:
                    fh.seek(0)
                    f.write(fh.read())

    def list_folders(self, context, filid, des):
        """
        List all items (file/subfolder) in the folder with given id
        until all files are found
        """

        with gdrive_client(self._config) as service:
            page_token = None
            while True:
                results = (
                    service.files()
                    .list(
                        pageSize=1000,
                        q=f"'{filid}' in parents",
                        fields="nextPageToken, files(id, name, mimeType)",
                    )
                    .execute()
                )
                page_token = results.get("nextPageToken", None)
                if page_token is None:
                    folder = results.get("files", [])
                    for item in folder:
                        if (
                            str(item["mimeType"])
                            == "application/vnd.google-apps.folder"
                        ):
                            if not os.path.isdir(des + "/" + item["name"]):
                                os.mkdir(des + "/" + item["name"])
                            self.list_folders(
                                context, item["id"], des + "/" + item["name"]
                            )
                        else:
                            self.download_files(context, item["id"], item["name"], des)
                            context.log.debug(f"Downloaded file {item['name']}")
                    break
            return folder

    def download_folders(self, context, dataframe: pl.DataFrame, tmp_folder_path: str):
        """
        Download all files in the folder with given id from gdrive
        """

        with gdrive_client(self._config) as service:
            for row in dataframe.rows(named=True):
                isbn, folder_id = row["BookISBN"], row["Link"].split("/")[-1]

                folder = service.files().get(fileId=folder_id).execute()
                folder_name = folder["name"]
                page_token = None

                while True:
                    results = (
                        service.files()
                        .list(
                            q=f"'{folder_id}' in parents",
                            spaces="drive",
                            fields="nextPageToken, files(id, name, mimeType)",
                        )
                        .execute()
                    )
                    page_token = results.get("nextPageToken", None)
                    if page_token is None:
                        items = results.get("files", [])
                        # If no items in the folder, that means the id is a file -> download the file
                        if not items:
                            self.download_files(context, folder_id, folder_name)
                            context.log.debug(f"Folder name: {folder_name}")
                        # If there are items in the folder -> download files in folder
                        else:
                            context.log.info(
                                f"Start downloading folder {folder_name} ..."
                            )

                            for item in items:
                                tmp_file_path = os.path.join(tmp_folder_path, str(isbn))
                                if not os.path.isdir(tmp_file_path):
                                    os.makedirs(tmp_file_path)
                                context.log.debug(f"Tmp file path: {tmp_file_path}")

                                file_path = ""
                                file_type = item["mimeType"]
                                context.log.debug(f"File type: {file_type}")
                                accept_files = ["jpeg", "epub"]
                                for accept_file in accept_files:
                                    if accept_file in file_type:
                                        file_path = os.path.join(
                                            tmp_file_path, item["name"]
                                        )

                                        context.log.debug(f"File path: {file_path}")
                                        self.download_files(
                                            context, item["id"], file_path
                                        )
                                        context.log.info(
                                            f"Downloaded file {item['name']}"
                                        )

                                        os.rename(
                                            file_path,
                                            os.path.join(
                                                tmp_file_path, f"{isbn}.{accept_file}"
                                            ),
                                        )
                    break

    def handle_output(self, context: "OutputContext", obj: dict):
        """
        Handle returned temporary folder path, load all files in the folder to minIO
        /tmp/bronze/download/2021-08-01T00:00:00+00:00
                                                    /images
                                                        ...jpeg
                                                    /files
                                                        ...epub
        to minIO:
        /lakehouse/images
        /lakehouse/files
        """

        tmp_folder_path = str(obj.get("tmp_folder_path"))
        isbn_list = obj.get("isbn")
        context.add_output_metadata({"tmp": tmp_folder_path})

        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                # Make bucket if not exist
                make_bucket(client, bucket_name)
                for isbn in isbn_list:
                    for filetype in ["epub", "jpeg"]:
                        # Upload epub file to minIO
                        # E.g /tmp/bronze/download/2021-08-01T00:00:00+00:00/123456/123456.epub/jpeg
                        tmp_file_path = os.path.join(
                            tmp_folder_path, str(isbn), f"{isbn}.{filetype}"
                        )
                        key_name = "files" if filetype == "epub" else "images"
                        key_name += f"/{isbn}.{filetype}"

                        # E.g bucket_name: lakehouse, key_name: files or images, tmp_file_path: /tmp/bronze/download/2021-08-01T00:00:00+00:00/123456/123456.epub/jpeg
                        client.fput_object(bucket_name, key_name, tmp_file_path)
                        context.log.debug(
                            f"(MinIO handle_output) Got book.{filetype} with isbn: {isbn}"
                        )

                        # Clean up tmp file
                        os.remove(tmp_file_path)
        except Exception as e:
            raise e

    def load_input(self, context: "InputContext"):
        """
        Skip this function
        """
        pass

#!/usr/bin/python
"""
WFP Global Market Monitor -  HDX Pipeline:
------------
"""

import logging
import os
from urllib.parse import urlsplit, unquote_plus

import pandas as pd
from datetime import datetime, timezone
from hdx.data.dataset import Dataset
from slugify import slugify

logger = logging.getLogger(__name__)


class WFPMarketMonitoring:
    def __init__(self, configuration, retriever, folder, errors):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.manual_url = None
        self.dataset_data = {}
        self.errors = errors
        self.created_date = None
        self.start_date = None
        self.latest_date = None

    def get_data(self, state):

        dataset_name = self.configuration["dataset_names"]["WFP-MARKET-MONITORING"]
        blob = self.configuration["blob"]

        try:
            url = os.environ["BLOB_URL"]
            account = os.environ["STORAGE_ACCOUNT"]
            container = os.environ["CONTAINER"]
            key = os.environ["KEY"]
        except Exception:
            url = self.configuration["url"]
            account = self.configuration["account"]
            container = self.configuration["container"]
            key = self.configuration["key"]

        logger.info(f"File url path: {url}")
        url2 = urlsplit(unquote_plus(url))
        logger.info(f"File url path: {url2}")

        downloaded_file = self.retriever.download_file(
            url=url,
            account=account,
            container=container,
            key=key,
            blob=blob)

        if blob.endswith("csv"):
            data_df = pd.read_csv(downloaded_file, sep=",", escapechar='\\', keep_default_na=False).replace('[“”]', '', regex=True)
        else:
            # they might send an excel
            data_df = pd.read_excel(downloaded_file).replace('[“”]', '', regex=True)

        data_df = data_df.rename(columns=lambda x: x.replace("MMFPSN", ""))

        # Find the minimum and maximum dates
        self.start_date = data_df['LastModifyDate'].min()
        self.latest_date = data_df['LastModifyDate'].max()

        hxl_tags = ["#date",
                    "#indicator+foodbasket+version",
                    "#meta+frequency",
                    "#country+code",
                    "#country+name",
                    "#adm1+name",
                    "#meta+level",
                    "#indicator+foodbasket+type",
                    "#indicator+foodbasket+price+type",
                    "#indicator+foodbasket+calories",
                    "#indicator+foodbasket+quarterly+change+nsa+num",
                    "#indicator+foodbasket+monthly+change+nsa+num",
                    "#indicator+foodbasket+quarterly+change+sa+num",
                    "#indicator+foodbasket+monthly+change+sa+num",
                    "#indicator+foodbasket+quarterly+change+yoy+num",
                    "#indicator+foodbasket+monthly+change+yoy+num",
                    "#indicator+foodbasket+quarterly+trend",
                    "#indicator+foodbasket+monthly+trend",
                    "#indicator+foodbasket+quarterly+share+num",
                    "#indicator+foodbasket+quarterly+change+num",
                    "#indicator+foodbasket+quarterly+change+code",
                    "#indicator+foodbasket+monthly+change+num",
                    "#indicator+foodbasket+monthly+change+code",
                    "#date+modified"]
        data_df.loc[-1] = hxl_tags
        data_df.index = data_df.index + 1
        data_df = data_df.sort_index()

        mask = data_df.DataLevel.str.contains("National")
        data_df_national = data_df[mask].reset_index(drop=True)
        data_df_national.loc[-1] = hxl_tags
        data_df_national.index = data_df_national.index + 1
        data_df_national = data_df_national.sort_index()
        data_df_subnational = data_df[~mask].reset_index(drop=True)
        self.dataset_data[dataset_name] = [data_df_national.apply(lambda x: x.to_dict(), axis=1),
                                           data_df_subnational.apply(lambda x: x.to_dict(), axis=1)]

        self.created_date = datetime.fromtimestamp((os.path.getctime(downloaded_file)), tz=timezone.utc)
        if self.created_date > state.get(dataset_name, state["DEFAULT"]):
            if self.dataset_data:
                return [{"name": dataset_name}]
            else:
                return None

    def generate_dataset(self, dataset_name):

        # Setting metadata and configurations
        name = self.configuration["dataset_names"]["WFP-MARKET-MONITORING"]
        title = self.configuration["title"]
        update_frequency = self.configuration["update_frequency"]
        dataset = Dataset({"name": slugify(name), "title": title})
        rows = self.dataset_data[dataset_name][0]
        dataset.set_maintainer(self.configuration["maintainer_id"])
        dataset.set_organization(self.configuration["organization_id"])
        dataset.set_expected_update_frequency(update_frequency)
        dataset.set_subnational(False)
        dataset["notes"] = self.configuration["notes"]
        filename = f"{dataset_name.lower()}.csv"
        resource_data = {"name": filename,
                         "description": self.configuration["description"]}
        tags = sorted([t for t in self.configuration["allowed_tags"]])
        dataset.add_tags(tags)

        # Setting time period
        start_date = self.start_date
        ongoing = False
        if not start_date:
            logger.error(f"Start date missing for {dataset_name}")
            return None, None
        dataset.set_time_period(start_date, self.latest_date, ongoing)

        headers = rows[0].keys()
        date_headers = [h for h in headers if "date" in h.lower() and type(rows[0][h]) == int]
        countrynames = []
        for row in rows:
            if row['CountryName'] != "#country+name":
                if row['CountryName'] == "Turkey":
                    countrynames.append("Türkiye")
                elif row['CountryName'] == "Cote d'Ivoire":
                    countrynames.append("Côte d'Ivoire")
                else:
                    countrynames.append(row['CountryName'])
            for date_header in date_headers:
                row_date = row[date_header]
                if not row_date:
                    continue
                if len(str(row_date)) > 9:
                    row_date = row_date / 1000
                row_date = datetime.utcfromtimestamp(row_date)
                row_date = row_date.strftime("%Y-%m-%d")
                row[date_header] = row_date

        for country in set(countrynames):
            dataset.add_other_location(country, exact=False)

        rows
        dataset.generate_resource_from_rows(
            self.folder,
            filename,
            rows,
            resource_data,
            list(rows[0].keys()),
            encoding='utf-8'
        )

        second_filename = f"{dataset_name.lower()}_subnational.csv"
        resource_data = {"name": second_filename,
                         "description": self.configuration["description_subnational_file"]}
        rows = self.dataset_data[dataset_name][1]
        headers = rows[0].keys()
        date_headers = [h for h in headers if "date" in h.lower() and type(rows[0][h]) == int]
        for row in rows:
            for date_header in date_headers:
                row_date = row[date_header]
                if not row_date:
                    continue
                if len(str(row_date)) > 9:
                    row_date = row_date / 1000
                row_date = datetime.utcfromtimestamp(row_date)
                row_date = row_date.strftime("%Y-%m-%d")
                row[date_header] = row_date

        rows
        dataset.generate_resource_from_rows(
            self.folder,
            second_filename,
            rows,
            resource_data,
            list(rows[0].keys()),
            encoding='utf-8'
        )

        return dataset
#!/usr/bin/python
"""Climate_trace scraper"""

import logging

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self.data = {}

    def get_data(self, end_year: int) -> None:
        base_url = self._configuration["base_url"]

        # list available gas parameters
        gas_url = self._configuration["gases"]
        gases = self._retriever.download_json(f"{base_url}{gas_url}")

        # loop through emissions sources endpoints
        endpoints = self._configuration["endpoints"]
        for data_type, endpoint_info in endpoints.items():
            self.data[data_type] = []
            min_year = endpoint_info["min_year"]
            for year in range(min_year, min_year + 1):
                for gas in gases[:1]:
                    for page in range(10000):
                        url = f"{base_url}{endpoint_info['url']}?year={year}&gas={gas}&limit=10000&offset={page * 10000}"
                        json = self._retriever.download_json(url)
                        if json is None:
                            break
                        self.data[data_type].append(json)
        return

    def process_data(self) -> list[str]:
        countries = []
        return countries

    def generate_country_dataset(self) -> Dataset | None:
        # To be generated
        dataset_name = None
        dataset_title = None
        dataset_time_period = None
        dataset_country_iso3 = None

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(dataset_time_period)
        dataset.add_tags(self._configuration["tags"])

        dataset.set_subnational(True)
        dataset.add_country_location(dataset_country_iso3)

        # Add resources here

        return dataset

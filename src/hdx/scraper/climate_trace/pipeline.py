#!/usr/bin/python
"""Climate_trace scraper"""

import logging
from datetime import datetime

from dateutil.relativedelta import relativedelta
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self.admins = {}
        self.data = {}

    def get_admin_data(self, iso3s: list[str]) -> None:
        base_url = self._configuration["admin_url"]
        for iso3 in iso3s:
            admin1_url = base_url.format(admin_id=iso3)
            try:
                admin1_json = self._retriever.download_json(admin1_url)
            except DownloadError:
                logger.error(f"No admin units found for {iso3}")
                continue
            admin0_info = {
                "id": iso3,
                "name": "",
                "full_name": "",
                "level": 0,
                "level_0_id": iso3,
                "level_1_id": "",
                "level_2_id": "",
            }
            self.admins[iso3] = {0: [admin0_info], 1: admin1_json, 2: []}
            for admin1_unit in admin1_json:
                admin_id = admin1_unit["level_1_id"]
                admin2_url = base_url.format(admin_id=admin_id)
                try:
                    admin2_json = self._retriever.download_json(admin2_url)
                except DownloadError:
                    continue
                self.admins[iso3][2].extend(admin2_json)
        return

    def fill_admin_info(self, admin_info: dict, admin_level: int) -> dict:
        keep_keys = [
            "level",
            "level_0_id",
            "level_0_name",
            "level_1_id",
            "level_1_name",
            "level_2_id",
            "level_2_name",
        ]
        admin_info["level_0_name"] = Country.get_country_name_from_iso3(
            admin_info["level_0_id"]
        )
        admin_info["level_1_name"] = ""
        admin_info["level_2_name"] = ""
        if admin_level == 1:
            admin_info["level_1_name"] = admin_info["name"]
        if admin_level == 2:
            admin_info["level_2_name"] = admin_info["name"]
            admin_1_units = self.admins[admin_info["level_0_id"]][1]
            admin_1_unit = [
                unit for unit in admin_1_units if unit["id"] == admin_info["level_1_id"]
            ][0]
            admin_info["level_1_name"] = admin_1_unit["name"]
        updated_admin_info = {key: admin_info[key] for key in keep_keys}
        return updated_admin_info

    def process_rows(
        self, input_data: dict, admin_info: dict, min_date: datetime
    ) -> list[dict]:
        rows = []
        min_year = min_date.year
        min_month = min_date.month
        sector_data = input_data["sectors"]["timeseries"]
        if sector_data is None:
            sector_data = []
        subsector_data = input_data["subsectors"]["timeseries"]
        if subsector_data is None:
            subsector_data = []
        for row in sector_data + subsector_data:
            if row["year"] <= min_year and row["month"] < min_month:
                continue
            new_row = admin_info | row
            rows.append(new_row)
        return rows

    def get_emissions_data(self, today: datetime) -> None:
        min_date = today - relativedelta(years=2)
        min_year = min_date.year
        max_year = today.year

        base_url = self._configuration["emissions_url"]

        # list available gas parameters
        gases = self._configuration["gases"]
        sectors = ",".join(self._configuration["sectors"])

        # loop through countries, admin units, gases, and years
        for iso3, admin_levels in self.admins.items():
            self.data[iso3] = {}
            for gas in gases:
                self.data[iso3][gas] = []
                for admin_level, admin_units in admin_levels.items():
                    for admin_unit in admin_units:
                        admin_info = self.fill_admin_info(admin_unit, admin_level)
                        admin_id = admin_info[f"level_{admin_level}_id"]

                        for year in range(min_year, max_year + 1):
                            url = f"{base_url}?sectors={sectors}&year={year}&gas={gas}&gadmId={admin_id}"
                            json = self._retriever.download_json(url)
                            rows = self.process_rows(json, admin_info, min_date)
                            self.data[iso3][gas].extend(rows)
        return

    def generate_country_dataset(self, iso3: str) -> Dataset | None:
        country_name = Country.get_country_name_from_iso3(iso3)
        dataset_name = f"{iso3.lower()}-climate-trace"
        dataset_title = f"{country_name}: {self._configuration['dataset_title']}"

        country_data = self.data.get(iso3)
        if country_data is None:
            return None

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        subnational = False
        dates = set()
        for gas, rows in country_data.items():
            for row in rows:
                date = f"{row['year']}-{row['month']}"
                dates.add(date)
                if not subnational and row["level"] > 0:
                    subnational = True
            resource_info = {
                "name": f"{iso3.lower()}_{gas}.csv",
                "description": "Emissions data for the past 24 months",
            }

            dataset.generate_resource(
                self._tempdir,
                resource_info["name"],
                rows,
                resource_info,
            )

        dataset.set_time_period(f"{min(dates)}-1")
        dataset.add_tags(self._configuration["tags"])

        dataset.set_subnational(subnational)
        dataset.add_country_location(iso3)

        return dataset

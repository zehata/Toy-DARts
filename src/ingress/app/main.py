from dataclasses import asdict
from jinja2 import Template
import psycopg2
from typing import Self
from dataclasses import dataclass
from zoneinfo import ZoneInfo
from logging import DEBUG
from logging import getLogger
from fastapi import UploadFile
from json import load
from datetime import datetime
from fastapi import FastAPI

# from starlette.requests import Request

logger = getLogger("uvicorn.error")
logger.setLevel(DEBUG)

app = FastAPI()
TIMEZONE = ZoneInfo("Asia/Singapore")

omop_concept_map: dict[str, dict[str, int]] = {
    "gender_concept_id": {
        "M": 8507,
        "F": 8532,
    },
    "race_concept_id": {
        "White": 8527,
    },
    "ethnicity_concept_id": {
        "Not Hispanic or Latino": 38003564,
    },
}


class VarChar50:
    _value_: str

    def __init__(self: Self, string: str):
        if len(string) > 50:
            raise ValueError("String too long")
        self._value_ = string

    def __str__(self: Self) -> str:
        return self._value_

    def __repr__(self: Self) -> str:
        return self._value_


@dataclass
class OmopPersonRow:
    person_id: int
    gender_concept_id: int
    year_of_birth: int
    month_of_birth: int
    day_of_birth: int
    race_concept_id: int
    birth_datetime: str | None = None
    ethnicity_concept_id: int | None = None
    location_id: int | None = None
    provider_id: int | None = None
    care_site_id: int | None = None
    person_source_value: VarChar50 | None = None
    gender_source_value: VarChar50 | None = None
    gender_source_concept_id: int | None = None
    race_source_value: VarChar50 | None = None
    race_source_concept_id: int | None = None
    ethnicity_source_value: VarChar50 | None = None
    ethnicity_source_concept_id: int | None = None


def get_omop_concept_id(type: str, concept: str):
    return omop_concept_map[type][concept]


def get_gender_assigned_at_birth(resource: dict):
    return resource["gender"]


type PosixTimestamp = int


def get_birthdate(year: int, month: int, day: int, tzinfo: ZoneInfo) -> PosixTimestamp:
    return int(datetime(year, month, day, tzinfo=ZoneInfo("Asia/Singapore")).timestamp())


extension_key_map = {
    "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race": "race",
    "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity": "ethnicity",
    "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex": "gender_assigned_at_birth",
}


def parse_extensions(extensions: list[dict]) -> dict[str, str]:
    extension_dict: dict[str, str] = {}
    for extension in extensions:
        match extension["url"]:
            case "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race":
                extension_dict["race"] = extension["extension"][1]["valueString"]
            case "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity":
                extension_dict["ethnicity"] = extension["extension"][1]["valueString"]
            case "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex":
                extension_dict["gender_assigned_at_birth"] = extension["valueCode"]
    return extension_dict


batch_insert_persons_template = """
    INSERT INTO person VALUES
        {% for person in persons %}
            ({{ person.person_id }}, {{ person.gender_concept_id }}, {{ person.year_of_birth }}, {{ person.month_of_birth }}, {{ person.day_of_birth }}, {{ person.birth_datetime }}, {{ person.race_concept_id }}, {{ person.ethnicity_concept_id }}, {{ person.location_id }}, {{ person.provider_id }}, {{ person.care_site_id }}, {{ person.person_source_value }}, {{ person.gender_source_value }}, {{ person.gender_source_concept_id }}, {{ person.race_source_value }}, {{ person.race_source_concept_id }}, {{ person.ethnicity_source_value }}, {{ person.ethnicity_source_concept_id }}){% if not loop.last %},{% endif %}
        {% endfor %};
"""


def convert_varchar_to_str(rows: dict):
    return {key: f"'{str(value)}'" if isinstance(value, VarChar50) else value for key, value in rows.items()}


def convert_none_to_null(rows: dict):
    return {key: "NULL" if value is None else value for key, value in rows.items()}


OMOP_CONCEPTS = omop_concept_map.keys()


def prepare_row(row: dict) -> dict:
    for key, value in row.items():
        match value:
            case str():
                row[key] = f"'{str(value)}'"
            case VarChar50():
                row[key] = f"'{str(value)}'"
            case None:
                row[key] = "NULL"
    return row


def get_batch_insert_persons_query(omop_person_rows: list[OmopPersonRow]) -> str:
    template = Template(batch_insert_persons_template)
    return template.render(persons=[prepare_row(asdict(row)) for row in omop_person_rows])


async def batch_insert_person_row(omop_person_rows: list[OmopPersonRow]):
    postgres_connection = psycopg2.connect(
        dbname="omop",
        user="postgres",
        password="postgres",
        host="omop-db",
    )
    postgres_cursor = postgres_connection.cursor()
    prepared_query = get_batch_insert_persons_query(omop_person_rows)
    print(prepared_query)
    postgres_cursor.execute(prepared_query)
    postgres_connection.commit()


@app.post("/ingress/fhir")
async def create_upload_file(file: UploadFile):
    patient_records: dict = load(file.file)
    entries = patient_records["entry"]
    omop_person_rows = []
    for entry in entries:
        resource = entry["resource"]
        match resource["resourceType"]:
            case "Patient":
                person_id = hash(resource["name"][0]["given"][0]) % 2147483647
                # we do not store patient name
                # how might we detect inadvertently added PHIs?

                extension_dict = parse_extensions(resource["extension"])

                gender_source_value = extension_dict["gender_assigned_at_birth"]
                race_source_value = extension_dict["race"]
                ethnicity_source_value = extension_dict["ethnicity"]

                gender_concept_id = get_omop_concept_id("gender_concept_id", gender_source_value)

                birthdate = resource["birthDate"]
                year, month, day = [int(birthdate_part) for birthdate_part in birthdate.split("-")]
                birth_datetime = birthdate

                race_concept_id = get_omop_concept_id("race_concept_id", race_source_value)
                ethnicity_concept_id = get_omop_concept_id("ethnicity_concept_id", ethnicity_source_value)

                omop_person_row = OmopPersonRow(
                    person_id=person_id,
                    gender_concept_id=gender_concept_id,
                    year_of_birth=year,
                    month_of_birth=month,
                    day_of_birth=day,
                    birth_datetime=birth_datetime,
                    race_concept_id=race_concept_id,
                    ethnicity_concept_id=ethnicity_concept_id,
                    gender_source_value=VarChar50(gender_source_value),
                    race_source_value=VarChar50(race_source_value),
                    ethnicity_source_value=VarChar50(ethnicity_source_value),
                )
                omop_person_rows.append(omop_person_row)
    await batch_insert_person_row(omop_person_rows)


# @app.post("/ingress/fhir/stream")
# async def stream_file(request: Request):
#     logger.info("Starting to stream")
#     data = cast("dict", json_stream.load(request.stream()))
#     for entry in data["entry"]:
#         logger.info(entry)

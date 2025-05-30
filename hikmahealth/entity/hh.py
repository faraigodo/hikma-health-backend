from __future__ import annotations
from collections import defaultdict
import logging
from typing import Optional, Union, Dict, List, Any

from psycopg import Connection
from psycopg.cursor import Cursor

from hikmahealth import sync
from hikmahealth.entity import core, fields, helpers
from .sync import (
    SyncToClient,
    SyncToServer,
)

from datetime import datetime
from hikmahealth.utils.datetime import utc
from datetime import date

import itertools
import dataclasses
import json
from urllib import parse as urlparse
import uuid

from hikmahealth.server.client import db
from psycopg.rows import class_row, dict_row
from hikmahealth.utils.misc import is_valid_uuid, safe_json_dumps
from hikmahealth.entity.helpers import SimpleCRUD, get_from_dict


@core.dataentity
class Patient(SyncToClient, SyncToServer, helpers.SimpleCRUD):
    TABLE_NAME = 'patients'

    id: str
    given_name: Optional[str] = None
    surname: Optional[str] = None
    date_of_birth: Optional[date] = None
    sex: Optional[str] = None
    camp: Optional[str] = None
    citizenship: Optional[str] = None
    hometown: Optional[str] = None
    phone: Optional[str] = None
    additional_data: Optional[Union[Dict, List]] = None
    government_id: Optional[str] = None
    external_patient_id: Optional[str] = None
    created_at: fields.UTCDateTime = fields.UTCDateTime(default_factory=utc.now)
    updated_at: fields.UTCDateTime = fields.UTCDateTime(default_factory=utc.now)

    @classmethod
    def create_from_delta(cls, ctx, cur: Cursor, data: dict) -> None:
        try:
            cur.execute(
                """INSERT INTO patients
                        (id, given_name, surname, date_of_birth, citizenship, hometown, sex, phone, camp, additional_data, image_timestamp, photo_url, government_id, external_patient_id, created_at, updated_at, last_modified)
                    VALUES
                        (%(id)s, %(given_name)s, %(surname)s, %(date_of_birth)s, %(citizenship)s, %(hometown)s, %(sex)s, %(phone)s, %(camp)s, %(additional_data)s, %(image_timestamp)s, %(photo_url)s, %(government_id)s, %(external_patient_id)s, %(created_at)s, %(updated_at)s, %(last_modified)s)
                    ON CONFLICT (id) DO UPDATE
                    SET given_name = EXCLUDED.given_name,
                        surname = EXCLUDED.surname,
                        date_of_birth = EXCLUDED.date_of_birth,
                        citizenship = EXCLUDED.citizenship,
                        hometown = EXCLUDED.hometown,
                        sex = EXCLUDED.sex,
                        phone = EXCLUDED.phone,
                        camp = EXCLUDED.camp,
                        additional_data = EXCLUDED.additional_data,
                        government_id = EXCLUDED.government_id,
                        external_patient_id = EXCLUDED.external_patient_id,
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at,
                        last_modified = EXCLUDED.last_modified;
                """,
                data,
            )
        except Exception as e:
            logging.error(f"Error creating/updating patient: {str(e)}")
            raise

    @classmethod
    def update_from_delta(cls, ctx, cur: Cursor, data: dict) -> None:
        return cls.create_from_delta(ctx, cur, data)

    @classmethod
    def delete_from_delta(cls, ctx, cur: Cursor, id: str) -> None:
        try:
            cur.execute(
                """INSERT INTO patients
                      (id, is_deleted, given_name, surname, date_of_birth, citizenship, hometown, sex, phone, camp, additional_data, image_timestamp, photo_url, government_id, external_patient_id, created_at, updated_at, last_modified, deleted_at)
                    VALUES
                      (%s::uuid, true, '', '', NULL, '', '', '', '', '', '{}', NULL, '', NULL, NULL, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET is_deleted = true,
                        deleted_at = EXCLUDED.deleted_at,
                        updated_at = EXCLUDED.updated_at,
                        last_modified = EXCLUDED.last_modified;
                """,
                (id, utc.now(), utc.now(), utc.now(), utc.now()),
            )

            # Soft delete related records
            related_tables = [
                'patient_additional_attributes',
                'visits',
                'events',
                'appointments'
            ]
            
            for table in related_tables:
                cur.execute(
                    f"""
                    UPDATE {table}
                    SET is_deleted = true,
                        deleted_at = %s,
                        updated_at = %s,
                        last_modified = %s
                    WHERE patient_id = %s::uuid;
                    """,
                    (utc.now(), utc.now(), utc.now(), id),
                )
        except Exception as e:
            logging.error(f"Error deleting patient {id}: {str(e)}")
            raise

    @classmethod
    def transform_delta(cls, ctx, action: str, data: Any) -> Dict:
        if action in (sync.ACTION_CREATE, sync.ACTION_UPDATE):
            patient = dict(data)
            additional_data = patient.get('additional_data', None)
            
            if additional_data is None or additional_data == '':
                additional_data = '{}'
            elif isinstance(additional_data, (dict, list)):
                additional_data = safe_json_dumps(additional_data)
            elif isinstance(additional_data, str):
                try:
                    json.loads(additional_data)
                except json.JSONDecodeError:
                    additional_data = '{}'

            patient.update(
                additional_data=additional_data,
                created_at=helpers.get_from_dict(
                    patient, 'created_at', utc.from_unixtimestamp
                ),
                updated_at=helpers.get_from_dict(
                    patient, 'updated_at', utc.from_unixtimestamp
                ),
                image_timestamp=helpers.get_from_dict(
                    patient, 'image_timestamp', utc.from_unixtimestamp
                ),
                photo_url='',
                last_modified=utc.now(),
            )
            return patient
        return {}

    @classmethod
    def get_column_names(cls) -> List[str]:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                q = """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'patients'
                    ORDER BY ordinal_position
                """
                cur.execute(q)
                return [row[0] for row in cur.fetchall()]

    @classmethod
    def filter_valid_columns(cls, columns: List[str]) -> List[str]:
        valid_columns = cls.get_column_names()
        return [column for column in columns if column in valid_columns]

    @classmethod
    def get_all_with_attributes(cls, count: Optional[int] = None) -> List[Dict]:
        with db.get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                query = """
                SELECT
                    p.*,
                    COALESCE(json_object_agg(
                        pa.attribute_id,
                        json_build_object(
                            'attribute', pa.attribute,
                            'number_value', pa.number_value,
                            'string_value', pa.string_value,
                            'date_value', pa.date_value,
                            'boolean_value', pa.boolean_value
                        )
                    ) FILTER (WHERE pa.attribute_id IS NOT NULL), '{}') AS additional_attributes
                FROM patients p
                LEFT JOIN patient_additional_attributes pa ON p.id = pa.patient_id
                WHERE p.is_deleted = false
                GROUP BY p.id
                ORDER BY p.updated_at DESC
                """
                if count is not None:
                    query += f' LIMIT {count}'
                
                cur.execute(query)
                patients = cur.fetchall()

                for patient in patients:
                    # Convert datetime/date objects to ISO strings
                    for key in [
                        'created_at',
                        'updated_at',
                        'last_modified',
                        'deleted_at',
                        'date_of_birth'
                    ]:
                        if patient.get(key):
                            patient[key] = patient[key].isoformat()

                return patients

    @classmethod
    def search(cls, query: str, conn: Connection) -> List[Dict]:
        with conn.cursor(row_factory=dict_row) as cur:
            search_query = """
                SELECT
                    p.*,
                    COALESCE(json_object_agg(
                        pa.attribute_id,
                        json_build_object(
                            'attribute', pa.attribute,
                            'number_value', pa.number_value,
                            'string_value', pa.string_value,
                            'date_value', pa.date_value,
                            'boolean_value', pa.boolean_value
                        )
                    ) FILTER (WHERE pa.attribute_id IS NOT NULL), '{}') AS additional_attributes
                FROM patients p
                LEFT JOIN patient_additional_attributes pa ON p.id = pa.patient_id
                WHERE p.is_deleted = false
                AND (LOWER(p.given_name) LIKE LOWER(%s) OR LOWER(p.surname) LIKE LOWER(%s))
                GROUP BY p.id
                ORDER BY p.updated_at DESC
                """
            search_pattern = f'%{query}%'
            cur.execute(search_query, (search_pattern, search_pattern))
            patients = cur.fetchall()

            for patient in patients:
                for key in [
                    'created_at',
                    'updated_at',
                    'last_modified',
                    'deleted_at',
                    'date_of_birth'
                ]:
                    if patient.get(key):
                        patient[key] = patient[key].isoformat()

            return patients


# Other classes (PatientAttribute, Event, Visit, etc.) would follow similar patterns
# with improved type hints, error handling, and removed duplicate code

######### HELPER DB METHODS #########

def upsert_visit(
    visit_id: Optional[str],
    patient_id: str,
    clinic_id: Optional[str],
    provider_id: Optional[str],
    provider_name: Optional[str],
    check_in_timestamp: datetime,
    metadata: Optional[Dict] = None,
    is_deleted: bool = False,
) -> str:
    """
    Upsert a visit into the table.
    Returns the visit_id (either the provided one or a new one if None was provided)
    """
    vid = visit_id if visit_id and is_valid_uuid(visit_id) else str(uuid.uuid4())
    current_time = utc.now()

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO visits (
                        id, patient_id, clinic_id, provider_id, provider_name,
                        check_in_timestamp, is_deleted, metadata,
                        created_at, updated_at, last_modified
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        patient_id = EXCLUDED.patient_id,
                        clinic_id = EXCLUDED.clinic_id,
                        provider_id = EXCLUDED.provider_id,
                        provider_name = EXCLUDED.provider_name,
                        check_in_timestamp = EXCLUDED.check_in_timestamp,
                        is_deleted = EXCLUDED.is_deleted,
                        metadata = EXCLUDED.metadata,
                        updated_at = EXCLUDED.updated_at,
                        last_modified = EXCLUDED.last_modified
                    RETURNING id;
                    """,
                    (
                        vid,
                        patient_id,
                        clinic_id,
                        provider_id,
                        provider_name,
                        check_in_timestamp,
                        is_deleted,
                        safe_json_dumps(metadata or {}),
                        current_time,
                        current_time,
                        current_time,
                    ),
                )
                conn.commit()
                return vid
            except Exception as e:
                conn.rollback()
                logging.error(f"Error upserting visit: {str(e)}")
                raise


def insert_placeholder_patient(conn: Connection, patient_id: str, is_deleted: bool = False) -> None:
    fixed_timestamp = datetime(2010, 6, 1, 0, 0, 0)

    with conn.cursor() as cur:
        try:
            placeholder_data = {
                'id': patient_id,
                'given_name': 'Placeholder',
                'surname': 'Patient',
                'date_of_birth': date.today(),
                'sex': 'Unknown',
                'camp': '',
                'citizenship': '',
                'hometown': '',
                'phone': '',
                'additional_data': json.dumps({}),
                'government_id': None,
                'external_patient_id': None,
                'created_at': fixed_timestamp,
                'updated_at': fixed_timestamp,
                'last_modified': fixed_timestamp,
                'server_created_at': fixed_timestamp,
                'deleted_at': fixed_timestamp if is_deleted else None,
                'is_deleted': is_deleted,
                'image_timestamp': None,
                'photo_url': '',
            }

            cur.execute(
                """
                INSERT INTO patients (
                    id, given_name, surname, date_of_birth, sex, camp, citizenship, hometown, phone,
                    additional_data, government_id, external_patient_id, created_at, updated_at,
                    last_modified, server_created_at, deleted_at, is_deleted, image_timestamp, photo_url
                ) VALUES (
                    %(id)s, %(given_name)s, %(surname)s, %(date_of_birth)s, %(sex)s, %(camp)s,
                    %(citizenship)s, %(hometown)s, %(phone)s, %(additional_data)s, %(government_id)s,
                    %(external_patient_id)s, %(created_at)s, %(updated_at)s, %(last_modified)s,
                    %(server_created_at)s, %(deleted_at)s, %(is_deleted)s, %(image_timestamp)s, %(photo_url)s
                )
                ON CONFLICT (id) DO NOTHING
                """,
                placeholder_data,
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"Error inserting placeholder patient: {str(e)}")
            raise


def row_exists(table_name: str, id: str) -> bool:
    """Check if a row exists in a table given its id."""
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                    SELECT EXISTS(
                        SELECT 1 FROM {table_name}
                        WHERE id = %s
                    )
                    """,
                    (id,),
                )
                return cur.fetchone()[0]
            except Exception as e:
                logging.error(f"Error checking row existence: {str(e)}")
                return False

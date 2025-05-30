from abc import abstractmethod
from typing import Any, Callable, Optional, Dict, TypeVar, Type
from hikmahealth.server.client import db

from psycopg.rows import dict_row, class_row

TKey = TypeVar('TKey')
TValue = TypeVar('TValue')


def get_from_dict(
    d: Dict[TKey, TValue],
    key: TKey,
    transform: Optional[Callable[[TValue], Any]] = None,
    defaultValue: Optional[Any] = None,
) -> Any:
    value = d.get(key, None)
    if value is not None:
        if transform is not None:
            return transform(value)
        return value
    else:
        return defaultValue


class SimpleCRUD:
    """Utility class containing simple operations to make fetches on the application"""

    @property
    @abstractmethod
    def TABLE_NAME(self) -> str:
        """This refers to the name of the table associated with the entity"""
        raise NotImplementedError(
            f"Require {self.__class__.__name__}.TABLE_NAME to be defined"
        )

    @classmethod
    def from_id(cls: Type["SimpleCRUD"], id: str) -> Optional["SimpleCRUD"]:
        with db.get_connection().cursor(row_factory=class_row(cls)) as cur:
            node = cur.execute(
                f"SELECT * FROM {cls.TABLE_NAME} WHERE is_deleted = FALSE AND id = %s;",
                [id],
            ).fetchone()
        return node

    @classmethod
    def get_all(cls: Type["SimpleCRUD"]) -> list["SimpleCRUD"]:
        with db.get_connection().cursor(row_factory=class_row(cls)) as cur:
            nodes = cur.execute(
                f"SELECT * FROM {cls.TABLE_NAME} WHERE is_deleted = FALSE;"
            ).fetchall()
        return nodes

    @classmethod
    def get_many(cls: Type["SimpleCRUD"], limit: int) -> list["SimpleCRUD"]:
        with db.get_connection().cursor(row_factory=class_row(cls)) as cur:
            nodes = cur.execute(
                f"SELECT * FROM {cls.TABLE_NAME} WHERE is_deleted = FALSE;"
            ).fetchmany(limit)
        return nodes
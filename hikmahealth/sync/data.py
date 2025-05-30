from typing import Callable, Literal, TypeVar, Generic, List, Optional, Union

TCreate = TypeVar('TCreate')
TUpdate = TypeVar('TUpdate')
TDelete = TypeVar('TDelete')

TCreatedData = TypeVar('TCreatedData')
TUpdatedData = TypeVar('TUpdatedData')
TDeletedData = TypeVar('TDeletedData')

ACTION_CREATE = 'CREATE'
ACTION_UPDATE = 'UPDATE'
ACTION_DELETE = 'DELETE'

ActionType = Literal['CREATE', 'UPDATE', 'DELETE']


class DeltaData(Generic[TCreate, TUpdate, TDelete]):
    """Describes the data and how it should be synchronized."""

    def __init__(
        self,
        created: Optional[List[TCreate]] = None,
        updated: Optional[List[TUpdate]] = None,
        deleted: Optional[List[TDelete]] = None,
    ):
        self.created: List[TCreate] = created if created is not None else []
        self.updated: List[TUpdate] = updated if updated is not None else []
        self.deleted: List[TDelete] = deleted if deleted is not None else []

    def __iter__(self):
        for d in self.created:
            yield ACTION_CREATE, d

        for d in self.updated:
            yield ACTION_UPDATE, d

        for d in self.deleted:
            yield ACTION_DELETE, d

    def to_dict(self) -> dict:
        return dict(created=self.created, updated=self.updated, deleted=self.deleted)

    @property
    def size(self) -> int:
        return len(self.created) + len(self.updated) + len(self.deleted)

    def add(
        self,
        created: Optional[List[TCreatedData]] = None,
        updated: Optional[List[TUpdatedData]] = None,
        deleted: Optional[List[TDeletedData]] = None,
    ) -> 'DeltaData[Union[TCreate, TCreatedData], Union[TUpdate, TUpdatedData], Union[TDelete, TDeletedData]]':
        cr: List[Union[TCreate, TCreatedData]] = list(self.created)
        if created:
            cr.extend(created)

        ur: List[Union[TUpdate, TUpdatedData]] = list(self.updated)
        if updated:
            ur.extend(updated)

        dr: List[Union[TDelete, TDeletedData]] = list(self.deleted)
        if deleted:
            dr.extend(deleted)

        return DeltaData(cr, ur, dr)

    @property
    def is_empty(self) -> bool:
        return not (self.created or self.updated or self.deleted)

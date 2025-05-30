"""
Section to facilitate performing the sync operation.
To contain entity that's separate and testable.
"""

from abc import ABC, abstractmethod
from collections import OrderedDict
import datetime
from typing import Callable, Dict, Generic, TypeVar

from .data import DeltaData
from .errors import SyncPushError

TArgs = TypeVar('TArgs')

# Define the function signature for sync push functions
SyncPushFunction = Callable[[DeltaData, datetime.datetime, TArgs], None]
"""Function signature to facilitate data synchronization upon receiving changes / `DeltaData`."""


class ISyncPush(Generic[TArgs], ABC):
    """Abstract class required to implement methods that facilitate synchronization operation
    when receiving new data."""

    @classmethod
    @abstractmethod
    def apply_delta_changes(
        cls, deltadata: DeltaData, last_pushed_at: datetime.datetime, args: TArgs
    ) -> None:
        raise NotImplementedError(
            f'Requires that {cls.__name__} implements this to synchronize from client.'
        )


class ISyncPull(Generic[TArgs], ABC):
    """Abstract class required to implement methods when to facilitate fetching data to be
    synced upstream."""

    @classmethod
    @abstractmethod
    def get_delta_records(cls, last_sync_time: int | str, args: TArgs) -> DeltaData:
        """Return the difference in data that was created, updated, or deleted since
        the last sync time.

        Implement this to prevent the code base from growing unnecessarily."""
        raise NotImplementedError()


class Sink(Generic[TArgs]):
    """Manages the synchronization operation."""

    def __init__(self) -> None:
        # Holds operations (either callable functions or classes implementing ISyncPush)
        self._ops: Dict[str, SyncPushFunction | type[ISyncPush[TArgs]]] = OrderedDict()

    def add(self, key: str, sync_operation) -> None:
        """Adds a sync operation for the given key."""
        assert key not in self._ops, f"Key '{key}' already added to sink."

        if isinstance(sync_operation, type):
            # Ensure the class has and properly implements `apply_delta_changes`.
            assert hasattr(sync_operation, 'apply_delta_changes'), (
                'Object is missing the `apply_delta_changes` method.'
            )
            assert callable(getattr(sync_operation, 'apply_delta_changes')), (
                'Class `apply_delta_changes` is not a callable class method.'
            )
        else:
            # Check if the sync_operation is a valid function.
            assert callable(sync_operation), (
                'Operation is neither a `class` nor a `function`.'
            )

        self._ops[key] = sync_operation

    def remove(self, key: str) -> None:
        """Removes the sync operation registered under the given key."""
        if key in self._ops:
            del self._ops[key]

    def push(
        self,
        key: str,
        deltadata: DeltaData,
        last_synced_at: datetime.datetime,
        args: TArgs,
    ) -> None:
        """Pushes the delta operations to the available nodes using their keys."""
        try:
            operation = self._ops[key]

            if isinstance(operation, type):
                # Calls the method if it implements the ISyncPush interface.
                operation.apply_delta_changes(deltadata, last_synced_at, args)
            else:
                assert callable(operation), 'Somehow, the operation is not callable.'
                operation(deltadata, last_synced_at, args)

        except KeyError:
            print(f'WARN: Skipping sync for unknown key={key}')
            return
        except Exception as err:
            raise SyncPushError('Failed to perform sync operation', *err.args)

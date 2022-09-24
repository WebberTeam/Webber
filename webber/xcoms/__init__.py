"""
Webber's event-driven communications library based on SQLite3.

Current iteration dependent on Webber tasks running on a common machine.

Last Updated: pre-0.0.1
"""

import sqlite3 as _sqlite3
from sre_compile import isstring as _isstring
from uuid import UUID as _UUID, uuid1 as _uuid1
from pickle import loads as _loads, dumps as _dumps
from typing import Any as _Any, KeysView as _KeysView, Union as _Union

# A list of keys used by Webber's Context is available for developer reference.
context_keys = ('_context_key', '_resource', '_original_context', '_requested')


class Resource:
    """
    Resource class used to handle SQLite store for Webber's DAG executor.

    Currently, fixed/default name for store table is `datastore`.
    """
    _secret_key: str = None
    _datastore: str = None

    def __init__(self, secret_key: _UUID, datastore: str) -> None:
        """Creates temporary database for Executor event and uses Context UUID as secret key."""
        if not isinstance(secret_key, _UUID):
            TypeError(f"{secret_key}: Invalid type for secret key.")
        self._secret_key = str(secret_key)
        self._datastore = datastore
        with open(datastore, "wb") as file_d:
            file_d.write(b'')
            file_d.close()
        conn = _sqlite3.connect(datastore)
        conn.execute("CREATE TABLE `datastore` (`key` TEXT PRIMARY KEY, `value` BLOB)")
        conn.close()

    def set(self, key: str, name: str, value: _Any) -> None:
        """Adds key-value pairs to the event's temporary database. Context identifer required."""
        if key != self._secret_key:
            err_msg = "Security requirements failed."
            raise ValueError(err_msg)
        if not isinstance(name, str):
            err_msg = "All Resource keys must be Python strings."
            raise TypeError(err_msg)
        conn = _sqlite3.connect(self._datastore)
        blob = _dumps(value, protocol=0) if not isinstance(value, bytes) else value
        conn.execute("INSERT INTO `datastore` VALUES(?, ?)", (name, blob))
        conn.commit()
        conn.close()


    def get(self, key: str, name: str) -> _Union[bytes, None]:
        """
        Gets the byte-string value associated with a requested key in the store.

        Returns `None` if the requested value does not exist.
        """
        if key != self._secret_key:
            err_msg = "Security requirements failed."
            raise ValueError(err_msg)
        conn = _sqlite3.connect(self._datastore)
        results = conn.execute("SELECT * FROM `datastore` WHERE `key` = (?)", (name,)).fetchall()
        if len(results) == 0:
            return None
        value: bytes = results[0][1]
        return value

    def secret_key(self):
        """Return's Resource's secret key for legal access of protected member."""
        return self._secret_key


class Promise:
    """Context for Webber tasks."""
    context_keys = ('_context_key', '_resource', '_original_context', '_requested')

    def __init__(self, *args, _is_collect=False) -> None:
        """
        Initializes a Python dictionary to recieve/use global data from executor.
        """
        self._dict = {}
        if _is_collect is False:
            if set(map(_isstring, args)).issuperset({False}):
                err_msg = "All requested keys must be Python strings."
                raise ValueError(err_msg)
            self._dict['_requested'] = list(args)

    def keys(self, include_context=True) -> _KeysView[str]:
        """
        Get a list of keys present in the Promise's dictionary.

        Includes context-defined keys by default.

        `include_context`: (bool) can be set to False for local-only view. (Default: True)
        """
        if include_context:
            return self._dict.keys()
        return dict.fromkeys(set(self._dict.keys()).difference(set(self.context_keys))).keys()

    def get(self, __key) -> _Any:
        """Get values assigned to string keys in the Promise's dictionary."""
        return self._dict.get(__key)

    def __getitem__(self, __key) -> _Any:
        """
        Get values assigned to string keys in the Promise's dictionary. >> v = p[k]

        Raises `KeyError` when given undefined keys.
        """
        return self._dict[__key]

    def __setitem__(self, __key: str, __value, **kwargs) -> None:
        """Set key-value pairs in the Promise's dictionary >> p[k] = v"""
        if not isinstance(__key, str):
            err_msg = "All uploaded keys must be Python strings."
            raise ValueError(err_msg)
        if __key in self.context_keys and kwargs.get('_force') is not True:
            err = f"Illegal setting of a Webber Task context key, {__key}."
            raise KeyError(err)
        self._dict[__key] = __value

    set = __setitem__

    def setdict(self, _dict: dict) -> None:
        """Set/overwrite Promise's internal dictionary."""
        self._dict = _dict

    def _exists(self, _key: str) -> bool:
        """Returns whether or not a key-value pair exists in the Promise's dictionary."""
        try:
            _ = self[_key]
            return True
        except KeyError:
            return False

    def _changed_pair(self, _key: str) -> bool:
        """Returns whether or not a key-value pair has been changed from the original context."""
        try:
            init_value = self['_original_context'][_key]
        except KeyError:
            return True
        return self[_key] != init_value

    def __str__(self):
        return str(self._dict)

    def upload(self) -> None:
        """
        On call, upload all new/local key-value pairs to the event-wide Resource.
        """
        # Base case: No need to conduct upload!
        if set(self.context_keys).issuperset(set(self.keys())):
            return
        if set(map(self._exists, self.context_keys)).issuperset({False}):
            err_msg = "Runtime context has been improperly removed/mutated."
            raise RuntimeError(err_msg)
        init_keys = set(self.keys()).difference(set(self.context_keys))
        upload_keys = [key for key in init_keys if self._changed_pair(key)]
        resource: Resource = self['_resource']
        for key in upload_keys:
            resource.set(resource.secret_key(), key, _dumps(self[key], protocol=0))


class Context(dict):
    """
    Context for Webber DAG Executor events.

    Initializes a Python dictionary to share event-wide data with Webber tasks.
    """
    def __init__(self, datastore: str) -> None:
        """Initializes a Python dictionary to share event-wide data with Webber tasks."""
        # super(Context, self).__init__()
        super().__init__()
        if not isinstance(datastore, str):
            err_msg = "Invalid datastore requested."
            raise ValueError(err_msg)
        self['_context_key'] = str(_uuid1())
        self._resource = Resource(self['_context_key'], datastore)
        self['_resource'] = self._resource

    def collect(self, promise) -> Promise:
        """
        Deliver event context to the Webber task as a 'fulfilled' Promise.

        If additional keys have been requested, search the event Resource for any matching
        key-value pairs and return the data.
        """
        guarantee = Promise(_is_collect=True)
        guarantee.setdict(dict(self.items()))
        requested_keys = promise['_requested']
        guarantee.__setitem__('_requested', promise['_requested'], _force=True)             # pylint: disable=unnecessary-dunder-call
        original_context = {}
        # Now, we reach out to the Context's Resource for the key-value pairs.
        for req_key in requested_keys:
            req_val = self._resource.get(key=self['_context_key'], name=req_key)            # pylint: disable=unnecessary-dunder-call
            guarantee[req_key] = _loads(req_val) if req_val is not None else req_val
            original_context[req_key] = guarantee[req_key]
        guarantee.__setitem__('_original_context', original_context, _force=True)           # pylint: disable=unnecessary-dunder-call
        return guarantee

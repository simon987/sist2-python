import json
import os
import sqlite3
import struct
from collections import namedtuple
from typing import List

_Sist2Version = namedtuple("Sist2Version", (
    "id", "date"
))


class Sist2Version(_Sist2Version):
    """
    Sist2 index version. (starts at version 1, is incremented by one for each incremental scan)
    """


_Sist2Descriptor = namedtuple("Sist2Descriptor", (
    "id", "version_major", "version_minor", "version_patch", "root", "name", "rewrite_url", "timestamp"
))


class Sist2Descriptor(_Sist2Descriptor):
    """
    Sist2 index descriptor
    """


_Sist2Document = namedtuple("Sist2Document", (
    "id", "version", "mtime", "size", "json_data", "rel_path", "path"
))


class Sist2Document(_Sist2Document):
    """
    Sist2 document - instantiated by sist2.Sist2Index.document_iter
    """


class Sist2Index:

    def __init__(self, filename: str):
        """
        :param filename: path to the sist2 index
        """
        self.filename = filename
        self.conn = sqlite3.connect(filename)
        self.cur = self.conn.cursor()
        self.last_id = None
        self._descriptor = self._get_descriptor()
        self._versions = self._get_versions()
        self._setup_kv()

    @property
    def descriptor(self) -> Sist2Descriptor:
        """
        :return: Index descriptor
        """
        return self._descriptor

    @property
    def versions(self) -> List[Sist2Version]:
        """
        Get index version history (starts at 1, is incremented after each incremental scan)
        """
        return self._versions

    def _setup_kv(self):
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS kv ("
            "   key TEXT PRIMARY KEY,"
            "   value TEXT"
            ")"
        )

    def get(self, key: str, default=None):
        """
        Get value from key-value table. This is used to store configuration or state in user scripts.

        :param key: Key
        :param default: Default value to return if not found
        :return: Value or default
        """

        self.cur.execute(
            "SELECT value from kv WHERE key=?", (key,)
        )

        row = self.cur.fetchone()
        if row:
            return row[0]

        return default

    def set(self, key: str, value: str | int) -> None:
        """
        Set value in key-value table.

        :param key: Key
        :param value: Value
        """

        self.cur.execute(
            "REPLACE INTO kv (key, value) VALUES (?,?)", (key, value)
        )

        return None

    def _get_descriptor(self) -> Sist2Descriptor:
        self.cur.execute(
            "SELECT id, version_major, version_minor, version_patch, root, name, rewrite_url, timestamp FROM descriptor"
        )

        return Sist2Descriptor(*self.cur.fetchone())

    def _get_versions(self) -> list:
        self.cur.execute(
            "SELECT id, date FROM version ORDER BY id"
        )

        return [
            Sist2Version(*row)
            for row in self.cur.fetchall()
        ]

    def get_thumbnail(self, id: str) -> bytes | None:
        """
        :param id: Document id
        :return: Thumbnail data
        """
        self.cur.execute(
            f"SELECT data from thumbnail WHERE id=?",
            (id,)
        )

        row = self.cur.fetchone()

        if not row:
            return None

        return row[0]

    def document_count(self, where: str = "") -> int:
        """
        Count the number of documents in the index

        :param where: SQL WHERE clause (ex. 'size > 100')
        :return: Number of documents in the index
        """

        if where:
            where = f"WHERE {where}"

        self.cur.execute(
            f"SELECT COUNT(*) FROM document"
            f" {where}"
        )

        row = self.cur.fetchone()
        return row[0]

    def document_iter(self, where: str = ""):
        """
        Iterate documents

        :param where: SQL WHERE clause (ex. 'size > 100')
        :return: generator
        """
        self.last_id = None

        doc = self._get_next_doc(where)
        while doc:
            yield doc
            doc = self._get_next_doc(where)

    def _get_next_doc(self, where=""):
        if self.last_id is None:
            where = f"WHERE {where}" if where else ""
            args = []
        elif where:
            where = f"WHERE document.id > ? AND ({where})"
            args = (self.last_id,)
        else:
            where = f"WHERE document.id > ?"
            args = (self.last_id,)

        self.cur.execute(
            f"SELECT document.id, version, mtime, size, json_data FROM document"
            f" {where}"
            f" ORDER BY document.id LIMIT 1",
            args
        )

        row = self.cur.fetchone()
        if not row:
            return None

        j = json.loads(row[4])
        rel_path = os.path.join(j["path"], j["name"] + ("." + j["extension"] if j["extension"] else ""))
        path = os.path.join(self.descriptor.root, j["path"],
                            j["name"] + ("." + j["extension"] if j["extension"] else ""))

        self.last_id = row[0]

        return Sist2Document(row[0], row[1], row[2], row[3], j, rel_path, path)

    def register_model(self, id: int, name: str, url: str, path: str, size: int, type: str) -> None:
        """
        Register a machine learning model for this index.

        :param id: Model ID,
        :param name: Name of the model, must be maximum 15 characters
        :param url: HTTP(s) url to the model for inference in the web UI, in .onnx format.
        :param path: Elasticsearch path. Must begin with `idx_512.` for indexed dense vector (max 1024-dim) or `512.` for dense vectors (replace 512 with the size).
        :param size: Size of the embedding in dimensions.
        :param type: Must be either 'flat' (one embedding per document) or 'nested' (multiple embeddings per document).
        """
        self.cur.execute(
            "REPLACE INTO model (id, name, url, path, size, type) VALUES (?,?,?,?,?,?)",
            (id, name, url, path, size, type)
        )

    def upsert_embedding(self, id: str, start: int, end: int | None, model_id: int, embedding: bytes) -> None:
        """
        Upsert an embedding

        :param id: Document ID
        :param start: Start offset in .content
        :param end: (optional) End offset in .content
        :param model_id: Model ID
        :param embedding: Encoded float32 embeddings (use serialize_float_array() to convert)
        """
        self.cur.execute(
            """
            REPLACE INTO embedding (id, start, end, model_id, embedding) VALUES (?,?,?,?,?)
            """,
            (id, start, end, model_id, embedding)
        )

    def update_document(self, doc: Sist2Document) -> None:
        """
        Update a document

        :param doc: document
        """
        self.cur.execute(
            """
            UPDATE document SET mtime=?, size=?, json_data=?
            WHERE id=?
            """,
            (doc.mtime, doc.size, json.dumps(doc.json_data), doc.id)
        )

    def sync_tag_table(self) -> None:
        """
        Update the tags table.
        You must call this function for tag filtering to function when using the SQLite search backend.
        This has no effect when using a ElasticSearch backend
        """
        self.cur.execute("DELETE FROM tag")
        self.cur.execute(
            "REPLACE INTO tag SELECT document.id, json_each.value FROM document, json_each(document.json_data->>'tag')")

    def commit(self) -> None:
        """
        Commit changes to the database
        """
        self.conn.commit()


def serialize_float_array(array) -> bytes:
    """
    :param array: float32 array (numpy etc.)
    :return: Encoded bytes, suitable for the embeddings table in sist2
    """
    return b''.join(
        struct.pack("f", x)
        for x in array
    )


def print_progress(done: int = 0, count: int = 0, waiting: bool = False) -> None:
    """
    Send current progress to sist2-admin. It will be displayed in the Tasks page

    :param done: Number of files processed
    :param count: Total number of files to process (including files that have been processed)
    :param waiting: Whether the script is still discovering new files to process
    """

    progress = {
        "done": done,
        "count": count,
        "waiting": waiting
    }

    print(f"$PROGRESS {json.dumps(progress)}")

import functools as ft
import hashlib
import threading
import time
from typing import Any, Awaitable, Dict, Generator, List, Optional
from typing.io import IO as Stream

from acdcli.api import client as ACD
from acdcli.api.common import RequestError
from acdcli.cache import db as DB
from acdcli.cache.query import Node
from tornado import locks as tl
from wcpan.logger import INFO, EXCEPTION, DEBUG
import wcpan.worker as ww


MD5Hash = bytes
CheckPoint = Any
NodeID = str
ChangedLines = Generator['ChangeSet', None, None]


class ACDController(object):

    def __init__(self, auth_path: str) -> None:
        self._db = ACDDBController(auth_path)
        self._network = ACDClientController(auth_path)

    def close(self) -> None:
        if self._network:
            self._network.close()
            self._network = None
        if self._db:
            self._db.close()
            self._db = None

    async def sync(self) -> Awaitable[bool]:
        INFO('wcpan.acd') << 'syncing'

        check_point = await self._db.get_checkpoint()
        f = await self._network.get_changes(checkpoint=check_point,
                                            include_purged=bool(check_point))

        try:
            full = False
            first = True

            for changeset in await self._network.iter_changes_lines(f):
                if changeset.reset or (full and first):
                    await self._db.reset()
                    full = True
                else:
                    await self._db.remove_purged(changeset.purged_nodes)

                if changeset.nodes:
                    await self._db.insert_nodes(changeset.nodes,
                                                partial=not full)

                await self._db.update_last_sync_time()

                if changeset.nodes or changeset.purged_nodes:
                    await self._db.update_check_point(changeset.checkpoint)

                first = False
        except RequestError as e:
            EXCEPTION('wcpan.acd') << str(e)
            return False

        INFO('wcpan.acd') << 'synced'

        return True

    async def trash(self, node_id: NodeID) -> Awaitable[bool]:
        try:
            r = await self._network.move_to_trash(node_id)
        except RequestError as e:
            EXCEPTION('wcpan.acd') << str(e)
            return False

        await self._db.insert_nodes([r])
        return True

    async def create_directory(self, node: Node,
                               name: str) -> Awaitable[Optional[Node]]:
        try:
            r = await self._network.create_directory(node, name)
        except RequestError as e:
            EXCEPTION('wcpan.acd') << str(e)
            return None

        await self._db.insert_nodes([r])
        r = await self._db.get_node(r['id'])
        return r

    async def download_node(self, node: Node,
                            local_path: str) -> Awaitable[MD5Hash]:
        return await self._network.download_node(node, local_path)

    async def upload_file(self, node: Node,
                          local_path: str) -> Awaitable[Optional[Node]]:
        r = await self._network.upload_file(node, local_path)
        await self._db.insert_nodes([r])
        r = await self._db.get_node(r['id'])
        return r

    async def resolve_path(self, remote_path: str) -> Awaitable[Optional[Node]]:
        return await self._db.resolve_path(remote_path)

    async def get_child(self, node: Node,
                        name: str) -> Awaitable[Optional[Node]]:
        return await self._db.get_child(node, name)

    async def get_children(self, node: Node) -> Awaitable[List[Node]]:
        return await self._db.get_children(node)

    async def get_path(self, node: Node) -> Awaitable[str]:
        return await self._db.get_path(node)

    async def get_node(self, node_id: NodeID) -> Awaitable[Optional[Node]]:
        return await self._db.get_node(node_id)

    async def find_by_regex(self, pattern: str) -> Awaitable[List[Node]]:
        return await self._db.find_by_regex(pattern)


class ACDClientController(object):

    def __init__(self, auth_path: str) -> None:
        self._auth_path = auth_path
        self._worker = ww.AsyncWorker()
        self._link = None

    def close(self) -> None:
        self._worker.stop()
        self._link = None

    async def create_directory(self, node: Node,
                               name: str) -> Awaitable[Dict[str, Any]]:
        await self._ensure_alive()
        fn = ft.partial(self._link.create_folder, name, node.id)
        rv = await self._worker.do(fn)
        return rv

    async def download_node(self, node: Node,
                            local_path: str) -> Awaitable[MD5Hash]:
        await self._ensure_alive()
        fn = ft.partial(self._download, node, local_path)
        rv = await self._worker.do(fn)
        return rv

    async def get_changes(self, checkpoint: CheckPoint,
                          include_purged: bool) -> Awaitable[Stream]:
        await self._ensure_alive()
        fn = ft.partial(self._link.get_changes, checkpoint=checkpoint,
                        include_purged=include_purged, silent=True, file=None)
        rv = await self._worker.do(fn)
        return rv

    async def iter_changes_lines(self,
                                 changes: Stream) -> Awaitable[ChangedLines]:
        await self._ensure_alive()
        rv = self._link._iter_changes_lines(changes)
        return rv

    async def move_to_trash(self, node_id: NodeID) -> Awaitable[Dict[str, Any]]:
        await self._ensure_alive()
        fn = ft.partial(self._link.move_to_trash, node_id)
        rv = await self._worker.do(fn)
        return rv

    async def upload_file(self, node: Node,
                          local_path: str) -> Awaitable[Dict[str, Any]]:
        await self._ensure_alive()
        fn = ft.partial(self._link.upload_file, str(local_path), node.id)
        rv = await self._worker.do(fn)
        return rv

    async def _ensure_alive(self) -> Awaitable[None]:
        if not self._link:
            self._worker.start()
            await self._worker.do(self._create_client)

    def _create_client(self) -> None:
        assert self._link is None
        self._link = ACD.ACDClient(self._auth_path)

    def _download(self, node: Node, local_path: str) -> MD5Hash:
        hasher = hashlib.md5()
        cb = [
            hasher.update,
        ]
        self._link.download_file(node.id, node.name, str(local_path),
                                 write_callbacks=cb)
        return hasher.hexdigest()


def off_main_thread(afn):
    @ft.wraps(afn)
    async def run_in_pool(self, *args, **kwargs):
        fn = ft.partial(afn, self, *args, **kwargs)
        return await self._pool.do(fn)
    return run_in_pool


class ACDDBController(object):

    # magic strings from acd_cli
    _CHECKPOINT_KEY = 'checkpoint'
    _LAST_SYNC_KEY = 'last_sync'

    def __init__(self, auth_path: str) -> None:
        self._auth_path = auth_path
        self._pool = ww.AsyncWorkerPool()
        # acd_cli uses thread local storage to isolate sqlite3 connection for
        # each thread, so it would be better to have individual database object
        # in each worker
        self._tls = threading.local()

    def close(self) -> None:
        self._pool.stop()
        # TODO cleanup for each worker

    @off_main_thread
    async def resolve_path(self, remote_path: str) -> Awaitable[Optional[Node]]:
        rv = self._db.resolve(remote_path)
        return rv

    @off_main_thread
    async def get_child(self, node: Node,
                        name: str) -> Awaitable[Optional[Node]]:
        child_node = self._db.get_child(node.id, name)
        return child_node

    @off_main_thread
    async def get_children(self, node: Node) -> Awaitable[List[Node]]:
        folders, files = self._db.list_children(node.id)
        children = folders + files
        return children

    @off_main_thread
    async def get_path(self, node: Node) -> Awaitable[str]:
        dirname = self._db.first_path(node.id)
        return dirname + node.name

    @off_main_thread
    async def get_node(self, node_id: NodeID) -> Awaitable[Optional[Node]]:
        rv = self._db.get_node(node_id)
        return rv

    @off_main_thread
    async def find_by_regex(self, pattern: str) -> Awaitable[List[Node]]:
        rv = self._db.find_by_regex(pattern)
        return rv

    @off_main_thread
    async def get_checkpoint(self) -> Awaitable[CheckPoint]:
        rv = self._db.KeyValueStorage.get(self._CHECKPOINT_KEY)
        return rv

    @off_main_thread
    async def reset(self) -> Awaitable[None]:
        self._db.drop_all()
        self._db.init()

    @off_main_thread
    async def remove_purged(self, nodes: List[NodeID]) -> Awaitable[None]:
        self._db.remove_purged(nodes)

    @off_main_thread
    async def insert_nodes(self, nodes: List[Dict[str, Any]],
                           partial: bool = True) -> Awaitable[None]:
        self._db.insert_nodes(nodes, partial=partial)

    @off_main_thread
    async def update_last_sync_time(self) -> Awaitable[None]:
        self._db.KeyValueStorage.update({
            self._LAST_SYNC_KEY: time.time(),
        })

    @off_main_thread
    async def update_check_point(self,
                                 check_point: CheckPoint) -> Awaitable[None]:
        self._db.KeyValueStorage.update({
            self._CHECKPOINT_KEY: check_point,
        })

    @property
    def _db(self):
        assert not is_main_thread()
        db = getattr(self._tls, 'db', None)
        if db is None:
            db = DB.NodeCache(self._auth_path)
            setattr(self._tls, 'db', db)
        return db


def is_main_thread():
    mt = threading.main_thread()
    ct = threading.current_thread()
    return mt.ident == ct.ident

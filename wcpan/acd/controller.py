import functools
import hashlib
import time

from acdcli.api import client as ACD
from acdcli.api.common import RequestError
from acdcli.cache import db as DB
from acdcli.cache.query import Node
from wcpan.logger import INFO, EXCEPTION, DEBUG
import wcpan.worker as ww


class ACDController(object):

    def __init__(self, auth_path):
        self._db = ACDDBController(auth_path)
        self._network = ACDClientController(auth_path)

    def close(self):
        if self._network:
            self._network.close()
            self._network = None
        if self._db:
            self._db.close()
            self._db = None

    async def sync(self):
        INFO('wcpan.acd') << 'syncing'

        check_point = await self._db.get_checkpoint()
        f = await self._network.get_changes(checkpoint=check_point, include_purged=bool(check_point))

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
                    await self._db.insert_nodes(changeset.nodes, partial=not full)

                await self._db.update_last_sync_time()

                if changeset.nodes or changeset.purged_nodes:
                    await self._db.update_check_point(changeset.checkpoint)

                first = False
        except RequestError as e:
            EXCEPTION('wcpan.acd') << str(e)
            return False

        INFO('wcpan.acd') << 'synced'

        return True

    async def trash(self, node_id):
        try:
            r = await self._network.move_to_trash(node_id)
            DEBUG('wcpan.acd') << r
            await self._db.insert_nodes([r])
        except RequestError as e:
            EXCEPTION('wcpan.acd') << str(e)
            return False
        return True

    async def create_directory(self, node, name):
        try:
            r = await self._network.create_directory(node, name)
            DEBUG('wcpan.acd') << r
        except RequestError as e:
            EXCEPTION('wcpan.acd') << str(e)
            return None
        await self._db.insert_nodes([r])
        r = await self._db.get_node(r['id'])
        return r

    async def download_node(self, node, local_path):
        return await self._network.download_node(node, local_path)

    async def upload_file(self, node, local_path):
        r = await self._network.upload_file(node, local_path)
        await self._db.insert_nodes([r])
        r = await self._db.get_node(r['id'])
        return r

    async def resolve_path(self, remote_path):
        return await self._db.resolve_path(remote_path)

    async def get_child(self, node, name):
        return await self._db.get_child(node, name)

    async def get_children(self, node):
        return await self._db.get_children(node)

    async def get_path(self, node):
        return await self._db.get_path(node)

    async def get_node(self, node_id):
        return await self._db.get_node(node_id)

    async def find_by_regex(self, pattern):
        return await self._db.find_by_regex(pattern)


class ACDClientController(object):

    def __init__(self, auth_path):
        self._auth_path = auth_path
        self._worker = ww.AsyncWorker()
        self._acd_client = None

    def close(self):
        self._worker.stop()
        self._acd_client = None

    async def create_directory(self, node, name):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_client.create_folder, name, node.id))

    async def download_node(self, node, local_path):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._download, node, local_path))

    async def get_changes(self, checkpoint, include_purged):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_client.get_changes, checkpoint=checkpoint, include_purged=include_purged, silent=True, file=None))

    async def iter_changes_lines(self, changes):
        await self._ensure_alive()
        return self._acd_client._iter_changes_lines(changes)

    async def move_to_trash(self, node_id):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_client.move_to_trash, node_id))

    async def upload_file(self, node, local_path):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_client.upload_file, str(local_path), node.id))

    async def _ensure_alive(self):
        if not self._acd_client:
            self._worker.start()
            await self._worker.do(self._create_client)

    def _create_client(self):
        self._acd_client = ACD.ACDClient(self._auth_path)

    def _download(self, node, local_path):
        hasher = hashlib.md5()
        self._acd_client.download_file(node.id, node.name, str(local_path), write_callbacks=[
            hasher.update,
        ])
        return hasher.hexdigest()


class ACDDBController(object):

    _CHECKPOINT_KEY = 'checkpoint'
    _LAST_SYNC_KEY = 'last_sync'

    def __init__(self, auth_path):
        self._auth_path = auth_path
        self._worker = ww.AsyncWorker()
        self._acd_db = None

    def close(self):
        self._worker.stop()
        self._acd_db = None

    async def resolve_path(self, remote_path):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_db.resolve, remote_path))

    async def get_child(self, node, name):
        await self._ensure_alive()
        child_node = await self._worker.do(functools.partial(self._acd_db.get_child, node.id, name))
        return child_node

    async def get_children(self, node):
        await self._ensure_alive()
        folders, files = await self._worker.do(functools.partial(self._acd_db.list_children, node.id))
        children = folders + files
        return children

    async def get_path(self, node):
        await self._ensure_alive()
        dirname = await self._worker.do(functools.partial(self._acd_db.first_path, node.id))
        return dirname + node.name

    async def get_node(self, node_id):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_db.get_node, node_id))

    async def find_by_regex(self, pattern):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_db.find_by_regex, pattern))

    async def get_checkpoint(self):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_db.KeyValueStorage.get, self._CHECKPOINT_KEY))

    async def reset(self):
        await self._ensure_alive()
        await self._worker.do(self._acd_db.drop_all)
        await self._worker.do(self._acd_db.init)

    async def remove_purged(self, nodes):
        await self._ensure_alive()
        await self._worker.do(functools.partial(self._acd_db.remove_purged, nodes))

    async def insert_nodes(self, nodes, partial=True):
        await self._ensure_alive()
        await self._worker.do(functools.partial(self._acd_db.insert_nodes, nodes, partial=partial))

    async def update_last_sync_time(self):
        await self._ensure_alive()
        await self._worker.do(functools.partial(self._acd_db.KeyValueStorage.update, {
            self._LAST_SYNC_KEY: time.time(),
        }))

    async def update_check_point(self, check_point):
        await self._worker.do(functools.partial(self._acd_db.KeyValueStorage.update, {
            self._CHECKPOINT_KEY: check_point,
        }))

    async def _ensure_alive(self):
        if not self._acd_db:
            self._worker.start()
            await self._worker.do(self._create_db)

    def _create_db(self):
        self._acd_db = DB.NodeCache(self._auth_path)

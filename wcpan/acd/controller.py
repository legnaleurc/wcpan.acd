import functools
import hashing

from acdcli.api import client as ACD
from acdcli.cache import db as DB
import wcpan.worker as ww


class ACDController(object):

    def __init__(self, auth_path):
        self._worker = ww.AsyncWorker()
        self._db = ACDDBController(auth_path)
        self._network = ACDClientController(auth_path)

    def close(self):
        self._network.close()
        self._db.close()
        self._worker.stop()


class ACDClientController(object):

    def __init__(self, auth_path):
        self._auth_path = auth_path
        self._worker = ww.AsyncWorker()
        self._acd_client = None

    def close(self):
        self._worker.stop()
        self._acd_client = None

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

    async def sync(self):
        INFO('acddl') << 'syncing'

        await self._ensure_alive()

        # copied from acd_cli

        check_point = await self._worker.do(functools.partial(self._acd_db.KeyValueStorage.get, self._CHECKPOINT_KEY))

        f = await self._acd_client.get_changes(checkpoint=check_point, include_purged=bool(check_point))

        try:
            full = False
            first = True

            for changeset in await self._acd_client.iter_changes_lines(f):
                if changeset.reset or (full and first):
                    await self._worker.do(self._acd_db.drop_all)
                    await self._worker.do(self._acd_db.init)
                    full = True
                else:
                    await self._worker.do(functools.partial(self._acd_db.remove_purged, changeset.purged_nodes))

                if changeset.nodes:
                    await self._worker.do(functools.partial(self._acd_db.insert_nodes, changeset.nodes, partial=not full))
                await self._worker.do(functools.partial(self._acd_db.KeyValueStorage.update, {
                    self._LAST_SYNC_KEY: time.time(),
                }))

                if changeset.nodes or changeset.purged_nodes:
                    await self._worker.do(functools.partial(self._acd_db.KeyValueStorage.update, {
                        self._CHECKPOINT_KEY: changeset.checkpoint,
                    }))

                first = False
        except RequestError as e:
            EXCEPTION('acddl') << str(e)
            return False

        INFO('acddl') << 'synced'

        return True

    async def _ensure_alive(self):
        if not self._acd_client:
            self._worker.start()
            await self._worker.do(self._create_client)

    def _create_client(self):
        self._acd_client = ACD.ACDClient(self._auth_path)

    def _download(self, node, local_path):
        hasher = hashing.IncrementalHasher()
        self._acd_client.download_file(node.id, node.name, str(local_path), write_callbacks=[
            hasher.update,
        ])
        return hasher.get_result()


class ACDDBController(object):

    _CHECKPOINT_KEY = 'checkpoint'
    _LAST_SYNC_KEY = 'last_sync'
    _MAX_AGE = 30

    def __init__(self, auth_path):
        self._auth_path = auth_path
        self._worker = worker.AsyncWorker()
        self._acd_db = None

    def close(self):
        self._worker.stop()
        self._acd_db = None

    async def resolve_path(self, remote_path):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_db.resolve, remote_path))

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

    async def trash(self, node_id):
        await self._ensure_alive()
        try:
            r = await self._context.client.move_to_trash(node_id)
            DEBUG('acddl') << r
            await self._worker.do(functools.partial(self._acd_db.insert_node, r))
        except RequestError as e:
            EXCEPTION('acddl') << str(e)
            return False
        return True

    async def _ensure_alive(self):
        if not self._acd_db:
            self._worker.start()
            await self._worker.do(self._create_db)

    def _create_db(self):
        self._acd_db = DB.NodeCache(self._auth_path)

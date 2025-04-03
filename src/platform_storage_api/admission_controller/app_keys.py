from aiohttp import web

from platform_storage_api.admission_controller.volume_resolver import KubeVolumeResolver
from platform_storage_api.storage import Storage


VOLUME_RESOLVER_KEY = web.AppKey("volume_resolver", KubeVolumeResolver)
STORAGE_KEY = web.AppKey("storage", Storage)

from aiohttp import web

from platform_storage_api.admission_controller.volume_resolver import KubeVolumeResolver


VOLUME_RESOLVER_KEY = web.AppKey("volume_resolver", KubeVolumeResolver)

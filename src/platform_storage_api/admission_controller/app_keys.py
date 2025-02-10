from aiohttp import web

from platform_storage_api.admission_controller.volume_resolver import KubeVolumeResolver


API_V1_KEY = web.AppKey("api_v1", web.Application)
VOLUME_RESOLVER_KEY = web.AppKey("volume_resolver", KubeVolumeResolver)

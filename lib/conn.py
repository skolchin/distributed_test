import ray
import logging
from pydantic import BaseModel, PrivateAttr
from lib.options import Options
from typing import Any

_logger = logging.getLogger(__name__)


class RayConnection(BaseModel):
    options: Options
    _client: Any = PrivateAttr(None)

    def _ray_init(self):
        address = self.options.cluster_address
        if not address:
            _logger.info('Using local Ray instance')
        else:
            if not address.partition(':')[2]:
                address = address + ':6379'
            _logger.info(f'Will use Ray cluster at {address}')

        ray_env = { k: str(v) for k, v in self.options.model_dump().items() if k.lower().startswith('ray_') }
        return ray.init(
            address=address,
            log_to_driver=True,
            runtime_env=ray_env,
            ignore_reinit_error=True,
        )


    def __enter__(self):
        self._client = self._ray_init()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # ray.shutdown()
        _logger.debug('Closing Ray connection')
        self._client.disconnect()

    async def __aenter__(self):
        self._client = self._ray_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # ray.shutdown()
        _logger.debug('Closing Ray connection')
        self._client.disconnect()

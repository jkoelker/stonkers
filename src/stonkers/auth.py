#

import logging
import os

from tda import auth  # type: ignore

LOG = logging.getLogger(__name__)


def get_client(api_key, redirect_uri, token_path, asyncio=False):
    if os.path.isfile(token_path):
        LOG.info("Returning client loaded from token file '%s'", token_path)
        return auth.client_from_token_file(
            token_path, api_key, asyncio=asyncio
        )

    return auth.client_from_manual_flow(
        api_key, redirect_uri, token_path, asyncio=asyncio
    )

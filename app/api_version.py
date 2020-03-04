from os import environ


AVAILABLE_API_VERSIONS = {'v1', 'v2'}


def get_api_versions() -> set:
    env = environ.get('API_VERSION', 'all').lower()

    if env == 'all':
        versions = AVAILABLE_API_VERSIONS
    else:
        versions = set(env.split(':'))
        for v in versions:
            if v not in AVAILABLE_API_VERSIONS:
                raise RuntimeError(f'API version {v} is not available')
        if not versions:
            raise RuntimeError(f'Specify at least one of available API versions: {AVAILABLE_API_VERSIONS}')

    return versions

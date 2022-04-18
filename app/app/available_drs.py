from collections.abc import Container
from os import environ
from typing import Union


class ContainsEverything(Container):
    def __contains__(self, item):
        return True


EVERYTHING = ContainsEverything()


def get_avail_drs() -> Union[set, ContainsEverything]:
    env = environ.get('AVAILABLE_DRS', 'all').lower()
    drs = set(env.split(':'))
    if 'all' in drs:
        return EVERYTHING
    return drs

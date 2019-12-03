from bxcommon.models.serializeable_enum import SerializeableEnum


class InstallationType(SerializeableEnum):
    DOCKER = "Docker"
    PYPI = "PyPI"

from enum import Enum
from typing import Any, Dict, Optional, Type

from pydantic import Extra  # pylint: disable=no-name-in-module

from ...utils.utils import BaseModel, ConfigurableClass, create_json_schema_conditionals
from .config import StringSource


class ComputeLogManagerType(str, Enum):
    NOOP = "NoOpComputeLogManager"
    LOCAL = "LocalComputeLogManager"  # deprecated in favor of noop
    AZURE = "AzureBlobComputeLogManager"
    GCS = "GCSComputeLogManager"
    S3 = "S3ComputeLogManager"
    CUSTOM = "CustomComputeLogManager"


class AzureBlobComputeLogManager(BaseModel):
    storageAccount: StringSource
    container: StringSource
    secretKey: StringSource
    localDir: Optional[StringSource]
    prefix: Optional[StringSource]


class GCSComputeLogManager(BaseModel):
    bucket: StringSource
    localDir: Optional[StringSource]
    prefix: Optional[StringSource]
    jsonCredentialsEnvvar: Optional[StringSource]


class S3ComputeLogManager(BaseModel):
    bucket: StringSource
    localDir: Optional[StringSource]
    prefix: Optional[StringSource]
    useSsl: Optional[bool]
    verify: Optional[bool]
    verifyCertPath: Optional[StringSource]
    endpointUrl: Optional[StringSource]
    skipEmptyFiles: Optional[bool]


class ComputeLogManagerConfig(BaseModel):
    azureBlobComputeLogManager: Optional[AzureBlobComputeLogManager]
    gcsComputeLogManager: Optional[GCSComputeLogManager]
    s3ComputeLogManager: Optional[S3ComputeLogManager]
    customComputeLogManager: Optional[ConfigurableClass]

    class Config:
        extra = Extra.forbid


class ComputeLogManager(BaseModel):
    type: ComputeLogManagerType
    config: ComputeLogManagerConfig

    class Config:
        extra = Extra.forbid

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type["ComputeLogManager"]):
            BaseModel.Config.schema_extra(schema, model)
            schema["allOf"] = create_json_schema_conditionals(
                {
                    ComputeLogManagerType.AZURE: "azureBlobComputeLogManager",
                    ComputeLogManagerType.GCS: "gcsComputeLogManager",
                    ComputeLogManagerType.S3: "s3ComputeLogManager",
                    ComputeLogManagerType.CUSTOM: "customComputeLogManager",
                }
            )

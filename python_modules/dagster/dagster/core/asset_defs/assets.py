import warnings
from typing import AbstractSet, Iterable, Mapping, Optional

from dagster import check
from dagster.core.definitions import OpDefinition
from dagster.core.definitions.events import AssetKey
from dagster.core.definitions.partition import PartitionsDefinition
from dagster.utils.backcompat import ExperimentalWarning

from .partition_mapping import PartitionMapping


class AssetsDefinition:
    def __init__(
        self,
        input_names_by_asset_key: Mapping[AssetKey, str],
        output_names_by_asset_key: Mapping[AssetKey, str],
        op: OpDefinition,
        partitions_def: Optional[PartitionsDefinition] = None,
        partition_mappings: Optional[Mapping[AssetKey, PartitionMapping]] = None,
    ):
        self._op = op
        self._input_defs_by_asset_key = {
            asset_key: op.input_dict[input_name]
            for asset_key, input_name in input_names_by_asset_key.items()
        }

        self._output_defs_by_asset_key = {
            asset_key: op.output_dict[output_name]
            for asset_key, output_name in output_names_by_asset_key.items()
        }
        self._partitions_def = partitions_def
        self._partition_mappings = partition_mappings or {}

    def __call__(self, *args, **kwargs):
        return self._op(*args, **kwargs)

    @property
    def op(self) -> OpDefinition:
        return self._op

    @property
    def asset_key(self) -> AssetKey:
        check.invariant(
            len(self._output_defs_by_asset_key) == 1,
            "Tried to retrieve asset key from an assets definition with multiple asset keys: "
            + ", ".join([str(ak.to_string()) for ak in self._output_defs_by_asset_key.keys()]),
        )

        return next(iter(self._output_defs_by_asset_key.keys()))

    @property
    def asset_keys(self) -> AbstractSet[AssetKey]:
        return self._output_defs_by_asset_key.keys()

    @property
    def output_defs_by_asset_key(self):
        return self._output_defs_by_asset_key

    @property
    def input_defs_by_asset_key(self):
        return self._input_defs_by_asset_key

    @property
    def partitions_def(self) -> Optional[PartitionsDefinition]:
        return self._partitions_def

    @property
    def dependency_asset_keys(self) -> Iterable[AssetKey]:
        return self._input_defs_by_asset_key.keys()

    def get_partition_mapping(self, in_asset_key: AssetKey) -> PartitionMapping:
        if self._partitions_def is None:
            check.failed("Asset is not partitioned")

        return self._partition_mappings.get(
            in_asset_key,
            self._partitions_def.get_default_partition_mapping(),
        )

    def with_replaced_asset_keys(
        self,
        output_asset_key_replacements: Mapping[AssetKey, AssetKey],
        input_asset_key_replacements: Mapping[AssetKey, AssetKey],
    ) -> "AssetsDefinition":
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=ExperimentalWarning)

            return self.__class__(
                input_names_by_asset_key={
                    input_asset_key_replacements.get(key, key): input_def.name
                    for key, input_def in self.input_defs_by_asset_key.items()
                },
                output_names_by_asset_key={
                    output_asset_key_replacements.get(key, key): output_def.name
                    for key, output_def in self.output_defs_by_asset_key.items()
                },
                op=self.op.with_replaced_asset_keys(
                    output_asset_key_replacements=output_asset_key_replacements,
                    input_asset_key_replacements=input_asset_key_replacements,
                ),
                partitions_def=self.partitions_def,
                partition_mappings=self._partition_mappings,
            )

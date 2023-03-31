from typing import TYPE_CHECKING, Optional, Union, Iterator, Tuple

from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata
from ray.data.context import DatasetContext
from ray.data.dataset_iterator import DatasetIterator
from ray.data._internal.stats import DatasetStats

if TYPE_CHECKING:
    import pyarrow
    from ray.data import Dataset


class DatasetIteratorImpl(DatasetIterator):
    def __init__(
        self,
        base_dataset: "Dataset",
    ):
        self._base_dataset = base_dataset
        self._base_context = DatasetContext.get_current()

    def __repr__(self) -> str:
        return f"DatasetIterator({self._base_dataset})"

    def _to_block_iterator(
        self,
    ) -> Tuple[
        Iterator[Tuple[ObjectRef[Block], BlockMetadata]], Optional[DatasetStats]
    ]:
        ds = self._base_dataset
        block_iterator, stats, executor = ds._plan.execute_to_iterator()
        ds._current_executor = executor
        return block_iterator, stats

    def stats(self) -> str:
        return self._base_dataset.stats()

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        return self._base_dataset.schema()

    def __getattr__(self, name):
        if name == "_base_dataset":
            raise AttributeError()

        if hasattr(self._base_dataset, name) and not name.startswith("_"):
            # Raise error for backwards compatibility.
            # TODO: remove this method in 2.6.
            raise DeprecationWarning(
                "session.get_dataset_shard returns a ray.data.DatasetIterator "
                "instead of a Dataset/DatasetPipeline as of Ray v2.3. "
                "Use iter_torch_batches(), to_tf(), or iter_batches() to "
                "iterate over one epoch. See "
                "https://docs.ray.io/en/latest/data/api/dataset_iterator.html "
                "for full DatasetIterator docs.",
            )

        raise AttributeError()
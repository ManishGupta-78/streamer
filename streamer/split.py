"""
Split iterable of records into batches with defined threshold
"""
from collections.abc import Iterable
from streamer import limits

def validate_config(max_record_size: int,
                    max_batch_size: int,
                    max_batch_len: int) -> None :
    """
    Validate splitter configuration
    """

    if max_batch_len <= 0:
        raise ValueError('Max batch length must be greater than zero')

    if max_record_size <= 0:
        raise ValueError('Max record size must be greater than zero')

    if max_batch_size < max_record_size:
        raise ValueError('Max batch size must be greater than or equal to max record size')

def split_gen(records: Iterable[str],
              max_record_size: int = limits.MAX_RECORD_SIZE,
              max_batch_size: int = limits.MAX_BATCH_SIZE,
              max_batch_len: int = limits.MAX_BATCH_LEN) -> Iterable[list[str]]:
    """
    Returns an iterator which yield batches from an array of records.
    """

    # Validate configuration
    validate_config(max_record_size, max_batch_size, max_batch_len)

    batch = []
    batch_size = 0

    # Iterate over input
    for record in records:
        record_len = len(record)

        # Record too large. Ignore
        if record_len > max_record_size:
            continue

        # Check if current record could fit into current batch
        if record_len + batch_size > max_batch_size:
            # Won't fit. Finalize batch
            yield batch

            # This record will now go in next batch
            batch = [record]
            batch_size = record_len
            continue

        # Add to existing batch
        batch.append(record)
        batch_size += record_len

        # Batch size limit reached
        if len(batch) == max_batch_len:
            yield batch
            batch = []
            batch_size = 0

    # Last batch
    if batch:
        yield batch

def split(records: Iterable[str],
          max_record_size: int = limits.MAX_RECORD_SIZE,
          max_batch_size: int = limits.MAX_BATCH_SIZE,
          max_batch_len: int = limits.MAX_BATCH_LEN) -> list[list[str]]:
    """
    Returns an array of batches for an array of records.
    """
    return list(split_gen(records, max_record_size, max_batch_size, max_batch_len))

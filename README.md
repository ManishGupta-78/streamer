
Purpose
-------
This python package allows spllitting of large record arrays into batches in accordance with specified limits.

Configuration
-------------
Following limits are supported

1. **Record size**
	Any record large than this size would be discarded.
	This must be greater than 0. An invalid limit will result in ValueError.
	Default value: 1MB

2. **Batch size**
	No more records would be added to a batch once this limit is reached.
    This must be greater than equal to record size. An invalid limit will result in ValueError.
	Default value: 5 MB

3. **Batch length**
	No more records would be added to a batch once number of records in current batch reaches this limit.
	This must be greater than 0. An invalid limit will result in ValueError.
	Default value: 500

API
---
1. split(records: Iterable[str], max_record_size: int, max_batch_size: int, max_batch_len: int) -> list[list[str]]

   split taken an iterable of records and returns a list of batches, where each batch is a list of records

    **Example**
    ```
	from streamer import split
	batches = split(['abc', 'def', 'ghijkl', 'mn', 'opqrstuvw', 'x', 'y', 'z'], 8, 3, 10)
	print(batches)
	>>> [['abc', 'def'], ['ghijkl', 'mn', 'x'], ['y', 'z']]  	
    ```
	**Explanation**:
	First split smade as we reached batch size limit
	Second split is made as we reached batch length limit
	'opqrstuvw' is ignored as it exceeded record size threshold

    **Warning**: This method returns all batches in one go and hence may not be suited for scenarios where we we have memory constraints and need to process a continous stream.

2. split_gen(records: Iterable[str], max_record_size: int, max_batch_size: int, max_batch_len: int) -> Iterator[list[str]]

   Returns an iterator which takes an iterable of records and generate batches of records.
   
   **Example**
   ```
	from streamer import split
	batches = split(['abc', 'def', 'ghijkl', 'mn', 'opqrstuvw', 'x', 'y', 'z'], 8, 3, 10)
	print(next(batches))
	>>> ['abc', 'def']
	print(next(batches))
	>>> ['ghijkl', 'mn', 'x']
	print(next(batches))
	>>> ['y', 'z']
	next(batches)
	>>> StopIteration
    ```
    This method is much more suited to be used for streaming batches as batches are generated on demand.

Limitations
-----------
This library is assuming 1 byte per character storage which may not hold true depending on character encoding.

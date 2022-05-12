# Cluster Aggregates Statistics

Script computes statistics (total number of keys, total memory-footprint) of a list of aggregates for a specific tier.

## Important notes

1. The script makes use of `rdbtool` and specifically, memory profiling - https://github.com/sripathikrishnan/redis-rdb-tools#generate-memory-report. As the document says, the memory usage here is approximate and the actual usage generally will be higher than reported usage
2. Memory usage includes memory usage of the key, value and overhead. Overhead might include TTL, hashtable entry, value reference storage etc. For more details see: https://github.com/sripathikrishnan/redis-rdb-tools/blob/548b11ec3c81a603f5b321228d07a61a0b940159/rdbtools/memprofiler.py#L438
3. Setting empty strings as key and value in redis still consume bytes purely due to overhead (~50 bytes). See: https://redis.io/commands/memory-usage/#examples
4. The tool reports the length of keys and values (since both of them are currently stored as strings). We could instead report the approx bytes used by each (using - https://github.com/sripathikrishnan/redis-rdb-tools/blob/548b11ec3c81a603f5b321228d07a61a0b940159/rdbtools/memprofiler.py#L422), but we can assume the memory usage can be further approximated (or use it relatively) with length

## Pre-requisites

### MemoryDB snapshots

Script requires the memorydb snapshots to compute statistics on. To get a snapshot of a MemoryDB cluster:

1. Manually create a snapshot of the cluster of interest. This can be done following the instructions here - https://docs.aws.amazon.com/memorydb/latest/devguide/snapshots-manual.html (use default encryption key)
2. Export the snapshot to a S3 bucket - this will allow downloading the snapshot and performing analysis over it. Follow instructions here - https://docs.aws.amazon.com/memorydb/latest/devguide/snapshots-exporting.html (NOTE: if you creating a new S3 bucket for this, ensure that ACLs are enabled and MemoryDB snapshot service access to write to the bucket).
3. Download the RDB files (one RDB file is created for each shard).

### Setup

Setup [rdbtools](https://github.com/sripathikrishnan/redis-rdb-tools).

In the `rexer` working directory, install `rdbtools`, `setuptools` and `python-lzf`. NOTE: `sudo` is required for nix. 

```
sudo pip3 install rdbtools setuptools python-lzf
```

## Running the script

### Folder structure

Script expects all the snapshots to analyze in a single directory and will create intermediate files in the same directory (creating sub-directories). Ideally you should create a directory for a MemoryDB cluster and load all it's snapshots in it.

### Statistics for chosen list of aggregates

```
go run -v -tags dynamic fennel/test/cmds/aggregate_stats --snapshot_dir {SNAPSHOT_DIR} --tier-id {TIER_ID} --aggregates [List of aggregate ids]
``` 

Running the script will create intermediate csv files. They will be stored in `{SNAPSHOT_DIR}/{AggId-csv}` for each `AggId` in `--aggregates`.

NOTE: These csv files will be re-used to compute statistics if the script is run again. Ideally for a new snapshot, a new directory should be created to avoid overlap and wrong measurements.

The output expected is of the format:

```
# [AggId] total keys: [# RedisKeys], size (MB): [Total size for that redis entry in MBs]

==========
AggId: 23
number of keys: 11281434
memory usage (MB): 3082
avg key length (NOTE: key is a string): 41.86
avg value length (NOTE: value is a string): 146.92
==========
```

### Statistics over all the aggregates

```
go run -v -tags dynamic fennel/test/cmds/aggregate_stats --snapshot_dir {SNAPSHOT_DIR} --tier-id {TIER_ID}
```

Running the script will create intermediate csv files. They will be stored in `{SNAPSHOT_DIR}/{all-csvs}`.

NOTE: These csv files will be re-used to compute statistics if the script is run again. Ideally for a new snapshot, a new directory should be created to avoid overlap and wrong measurements.

The output expected is of the format:

```
# [AggId] total keys: [# RedisKeys], size (MB): [Total size for that redis entry in MBs]

==========
AggId: 33
number of keys: 11281434
memory usage (MB): 3082
avg key length (NOTE: key is a string): 41.86
avg value length (NOTE: value is a string): 146.92
==========
==========
AggId: 10
number of keys: 11518
memory usage (MB): 1
avg key length (NOTE: key is a string): 29.86
avg value length (NOTE: value is a string): 47.06
==========
==========
AggId: 13
number of keys: 11773
memory usage (MB): 1
avg key length (NOTE: key is a string): 30.07
avg value length (NOTE: value is a string): 54.67
==========
==========
AggId: 30
number of keys: 1307853
memory usage (MB): 186
avg key length (NOTE: key is a string): 32.85
avg value length (NOTE: value is a string): 32.30
==========
==========
AggId: 18
number of keys: 51314538
memory usage (MB): 8659
avg key length (NOTE: key is a string): 38.57
avg value length (NOTE: value is a string): 50.15
==========
==========
AggId: 12
number of keys: 1439
memory usage (MB): 0
avg key length (NOTE: key is a string): 25.88
avg value length (NOTE: value is a string): 177.88
==========
==========
AggId: 20
number of keys: 1307853
memory usage (MB): 186
avg key length (NOTE: key is a string): 32.85
avg value length (NOTE: value is a string): 32.30
==========
==========
AggId: 17
number of keys: 15
memory usage (MB): 0
avg key length (NOTE: key is a string): 26.00
avg value length (NOTE: value is a string): 279.60
==========
==========
AggId: 11
number of keys: 17483
memory usage (MB): 2
avg key length (NOTE: key is a string): 30.81
avg value length (NOTE: value is a string): 30.78
==========
==========
AggId: 16
number of keys: 46
memory usage (MB): 0
avg key length (NOTE: key is a string): 25.83
avg value length (NOTE: value is a string): 79.96
==========
==========
AggId: 8
number of keys: 47574
memory usage (MB): 6
avg key length (NOTE: key is a string): 36.70
avg value length (NOTE: value is a string): 29.53
==========
==========
AggId: 28
number of keys: 51314538
memory usage (MB): 8659
avg key length (NOTE: key is a string): 38.57
avg value length (NOTE: value is a string): 50.15
==========
==========
AggId: 24
number of keys: 21373
memory usage (MB): 8
avg key length (NOTE: key is a string): 31.14
avg value length (NOTE: value is a string): 270.21
==========
==========
AggId: 14
number of keys: 4697
memory usage (MB): 1
avg key length (NOTE: key is a string): 32.00
avg value length (NOTE: value is a string): 115.09
==========
==========
AggId: 15
number of keys: 4548
memory usage (MB): 1
avg key length (NOTE: key is a string): 32.00
avg value length (NOTE: value is a string): 196.17
==========
==========
AggId: 29
number of keys: 13710438
memory usage (MB): 3465
avg key length (NOTE: key is a string): 41.26
avg value length (NOTE: value is a string): 128.14
==========
==========
AggId: 0
number of keys: 12658664
memory usage (MB): 1448
avg key length (NOTE: key is a string): 39.86
avg value length (NOTE: value is a string): 8.00
==========
==========
AggId: 19
number of keys: 13643388
memory usage (MB): 3447
avg key length (NOTE: key is a string): 41.26
avg value length (NOTE: value is a string): 128.07
==========
==========
AggId: 9
number of keys: 48011
memory usage (MB): 8
avg key length (NOTE: key is a string): 30.76
avg value length (NOTE: value is a string): 59.76
==========
==========
AggId: 23
number of keys: 11281434
memory usage (MB): 3082
avg key length (NOTE: key is a string): 41.86
avg value length (NOTE: value is a string): 146.92
==========
==========
AggId: 34
number of keys: 21813
memory usage (MB): 8
avg key length (NOTE: key is a string): 31.14
avg value length (NOTE: value is a string): 270.31
==========
```

NOTE: AggId = 0 is not defined. We store an entry in MemoryDB for deduplication. Those keys are represented as `0` here.

## Future Work

1. Automatically capture snapshots everyday with a retention limit of 1 day
2. Automatically export the latest snapshot to a s3 bucket
3. Compute the memory usage of the key and value, instead of just the length of them. This should be doable, using ref - https://github.com/sripathikrishnan/redis-rdb-tools/blob/548b11ec3c81a603f5b321228d07a61a0b940159/rdbtools/memprofiler.py#L196. Since both key and values are string, this should be as simple as implementing hereon - https://github.com/sripathikrishnan/redis-rdb-tools/blob/548b11ec3c81a603f5b321228d07a61a0b940159/rdbtools/memprofiler.py#L422

This should leave us just download the snapshots locally and running the script.

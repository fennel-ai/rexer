# Cluster Aggregates Statistics

Script computes statistics (total number of keys, total memory-footprint) of a list of aggregates for a specific tier.

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
[12] total keys: 1439, size (MB): 0
==========
==========
[14] total keys: 4697, size (MB): 1
==========
==========
[13] total keys: 11773, size (MB): 1
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

[20] total keys: 1307853, size (MB): 186
[17] total keys: 15, size (MB): 0
[12] total keys: 1439, size (MB): 0
[8] total keys: 47574, size (MB): 6
[29] total keys: 13710438, size (MB): 3465
[13] total keys: 11773, size (MB): 1
[16] total keys: 46, size (MB): 0
[9] total keys: 48011, size (MB): 8
[10] total keys: 11518, size (MB): 1
[33] total keys: 11281434, size (MB): 3082
[11] total keys: 17483, size (MB): 2
[23] total keys: 11281434, size (MB): 3082
[30] total keys: 1307853, size (MB): 186
[24] total keys: 21373, size (MB): 8
[34] total keys: 21813, size (MB): 8
[19] total keys: 13643388, size (MB): 3447
[15] total keys: 4548, size (MB): 1
[0] total keys: 12658664, size (MB): 1448
[18] total keys: 51314538, size (MB): 8659
[28] total keys: 51314538, size (MB): 8659
[14] total keys: 4697, size (MB): 1
```

NOTE: AggId = 0 is not defined. We store an entry in MemoryDB for deduplication. Those keys are represented as `0` here.

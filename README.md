# MyDataStore

MyDataStore is a leaderless distributed database designed for research and learning purposes, drawing inspiration from "Designing Data-Intensive Applications" by Martin Kleppmann. Key advantages include:

- **High Availability and Fault Tolerance**: The absence of a central leader node eliminates single point of failure and distributing data across multiple nodes, the system remains operational even if some nodes fail.
- **Leaderless Consensus**: Implmentation of the raft consensus algorithm is used to agree upon cluster organization.
- **Variable Consistency**: Read and write quorums are configurable, ensuring consistency, fast reads, or faster writes."

## Core Concepts

MyDataStore implements several core concepts from the book "Designing Data-Intensive Applications". These include:

1. **Node Discovery**:

   - Node discovery is implemented using a gossip protocol, which enables efficient and scalable membership validation and communication.

2. **Consistent Hashing**:

   - A hashring technique is used to evenly distribute data into partitions and distribute partitions across the cluster.
   - The cluster leader determins the member list, the following members replciate the hashring ensuring a consistent cluster hashing.

3. **Quorums and Read Repairs**:

   - The system uses a read quorum and a write quorum for operations.
   - - here N is number of replciated partitions, W is minimum nodes used to write, and R is minimum nodes used to read.
   - - R + W > N is used to ensure consistency and no stale reads.
   - - Lower W can be used for faster writes.
   - - Lower R can be used for faster reads.
   - Read repairs are used to write to nodes which return stale data in a read operation.

4. **Timestamps for Conflict Resolution**:

   - Timestamp-based conflict resolution is used to handle conflicts in the system.

5. **Using Raft for Consensus**:

   - Raft algorithm is used to elect a leader for the cluster through a democratic election process.
   - The elected leader maintains a log that is verified by the consensus algorithm to ensure data integrity and consistency.
   - In this project, the log is used to keep track of the members currently in the cluster and the current epoch of the cluster, providing a reliable source of truth for the system state.

6. **Periodic Partition Verification and Replication**:

   - Data replication and verification is a process that ensures data consistency and integrity across the distributed system.
   - It involves synchronizing partitions with epochs.
   - It creates Merkle trees for each owned partition.
   - It compares these trees across nodes.
   - It resolves inconsistencies by requesting specific buckets.
   - It updates data with bucket transfers.
   - The raft leader periodically increments a varaible called Epoch.
   - Epochs are stored with the value write.
   - Upon Epoch increment, each node creates a merkle tree for (Partition, Epoch, Data) then starts comparing the tree hash to the hash of other members.
   - Additional details:
   - - Merkle trees organize data into hashed buckets on key.
   - - Validation nodes will construct a remote tree with limied data of tree hash.
   - - - This is done so that all data does not need to be checked otherwise the process is redudant and takes a long time.
   - - BFS is used on local and remote tree to find buckets which are not in sync.
   - - A node can then request the out of sync bucket range instead of the whole partition. This speeds up the process of partition replication if out of sync. For a given partition and epoch, of a 1/64 values will need to be sync in this range.

7. **Data Storage Engine and Indexing**:
   - The system works with Badger and LevelDB as its data storage engines.
   - Both Badger and LevelDB are Log-Structured Merge-tree (LSM tree) based data structures, which provide high write throughput.
   - Indexes:
   - - (epoch, parition, key)
   - - (key)

## Development

### Prerequisites

- Bazel: Bazel is a build tool that is required for building and testing MyDataStore. You can install Bazel following the instructions on the [official Bazel website](https://bazel.build/).

### Dev Script

A dev script is provided to help with common commands. The script is written in bash and can be run from the command line. Here is a brief explanation of the commands supported by the script:

- `dc`: Starts a new tmuxinator session with the configuration specified in `tmuxinator-dc.yaml`.
- `dc!`: Kills the `dc_dev` tmux session.
- `k8-init`: Deletes and restarts minikube, enables the metrics-server addon, and runs the `k8-r` command.
- `k8-r`: Deletes the Kubernetes configuration specified in `./operator/config/samples/`, deploys the operator, and deploys Kubernetes.
- `k8-operator`: Deploys the operator.
- `k8`: Starts a new tmuxinator session with the configuration specified in `tmuxinator-k8.yaml`.
- `k8!`: Kills the `k8_dev` tmux session.
- `k8-e2e`: Runs end-to-end tests on Kubernetes.
- `dc-e2e`: Runs end-to-end tests using k6.
- `test`: Executes Bazel tests once. Accepts a test name as an argument for filtering.
- `rtest`: Uses `ibazel` to rerun tests on file change. Also accepts a test name as an argument for filtering.
- `deps`: Updates dependencies using `update_deps.sh`.
- `dc-pprof`: Starts a pprof profiling session.
- `scale`: Scales the number of replicas for the store. The number of replicas must be specified as an argument.

To run the script, use the following command:

```bash
./dev.sh <command> [arguments]
```

Replace <command> with one of the commands listed above, and [arguments] with any arguments required by the command.

# EDITOR SETUP

https://github.com/bazelbuild/rules_go/wiki/Editor-setup

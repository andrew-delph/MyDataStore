# my-key-store

my-key-store is a leaderless distributed database designed for research and learning purposes, drawing inspiration from "Designing Data-Intensive Applications" by Martin Kleppmann. Key advantages include:

- **High Availability and Fault Tolerance**: By avoiding a single point of failure and distributing data across multiple nodes, the system remains operational even if some nodes fail.
- **Scalability**: New nodes can be seamlessly integrated to accommodate increased data and traffic demands.
- **Consistent Performance**: The absence of a central leader node eliminates potential bottlenecks, ensuring steady system performance.
- **Ideal for Critical Applications**: The combination of availability, fault tolerance, and scalability makes my-key-store well-suited for applications with stringent reliability requirements.

## Core Concepts

my-key-store implements several core concepts from the book "Designing Data-Intensive Applications". These include:

1. **Gossip Protocol for Node Discovery**:

   - It is used for service discovery, helping nodes in the system to find each other.
   - It can determine when a node joins or leaves the cluster, updating the system's state accordingly.

2. **Hashring for Key Determination**:

   - A hashring technique is used to evenly distribute big data loads over many servers with a randomly generated key.
   - This consistently determines how to assign a piece of data to a specific processor.
   - The hashring assigns partitions to each node.
   - Given a key, the hashring can determine the partitions that own it.

3. **Read/Write Quorums and Read Repairs**:

   - The system uses a read quorum and a write quorum for operations.
   - A read quorum is the minimum number of nodes that must participate in a read operation.
   - A write quorum is the minimum number of nodes that must participate in a write operation.
   - Read repairs are used to maintain consistency in the system by actively comparing data from different replicas and triggering repairs when inconsistencies are detected.

4. **Timestamps for Conflict Resolution**:

   - Timestamp-based conflict resolution is used to handle conflicts in the system.
   - This method requires time synchronization across the system.
   - It ensures that the most recent version of any document will always win in case of conflicts.

5. **Using Raft for Consensus**:

   - Raft algorithm is used to elect a leader for the cluster through a democratic election process.
   - The elected leader maintains a log that is verified by the consensus algorithm to ensure data integrity and consistency.
   - In this project, the log is used to keep track of the members currently in the cluster and the current epoch of the cluster, providing a reliable source of truth for the system state.

6. **Data Replication and Verification**:

   - Data replication and verification is a process that ensures data consistency and integrity across the distributed system.
   - It involves synchronizing partitions with epochs.
   - It creates Merkle trees for each owned partition.
   - It compares these trees across nodes.
   - It resolves inconsistencies by requesting specific buckets.
   - It updates data with bucket transfers.

7. **Data Storage Engine**:
   - The system works with Badger and LevelDB as its data storage engines.
   - Both Badger and LevelDB are Log-Structured Merge-tree (LSM tree) based data structures, which provide high write throughput.

## Development

### Prerequisites

- Bazel: Bazel is a build tool that is required for building and testing my-key-store. You can install Bazel following the instructions on the [official Bazel website](https://bazel.build/).

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

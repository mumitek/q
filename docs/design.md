# Distributed Queue

Let's build a distributed queue service where you can push messages to and pop them back from it. It's should be available, reliable

## Idea

Messages are distributed to partitions, randomly. Each partition gets replicated to 3 different machines for durability. There's always a single primary replica, which receives the write requests. If a machine goes out, then for each dead primary replica, we merge the corresponding secondary replicas to some existing primary replica randomly. The controller triggers these merge actions. Once a new machine joins the cluster, it creates N partitions depending on its capacity.

## Technology choices

- Kubernetes to orchestrate the cluster
- Rust to implement
- gRPC for API calls


## Stages

### Not-so-good queue

[] API is defined
1. Kubernetes cluster configured
1. Each pod in the cluster manages N partitions, implemented by a simple file.
1. Push just pushes the message in the partition file, and updates the pointer
1. Pop just reads the message pointed by the pointer and updates the pointer

### Touch on reliability

1. Worker waits for client's acknowledgement before marking the message as read
1. Controller is introduced which keeps track of all nodes, and partition replicas
1. Can create / delete queue.
1. Each worker replicates it's partitions to other workers

### Make it faster

1. Keep a cache of the partition instead of writing from file each time
1. Better encoding of the data

### Faults in my cluster

1. Handle cases when a node is removed or added

## Tasks

[] Define API using protocol buffer
[] 


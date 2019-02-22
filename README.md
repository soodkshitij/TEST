# Distributed-Data-Store

This Repository contains code to set up a distributed data sharing system amongst multiple clusters. RAFT algorithm has been used as consensus algorithm. The cluster will have following capabilities :
   
    1. To store and pass messages between clusters in distributed fashion.
    2. Ability to fetch data even from private network in the cluster
    3. To build a fault-tolerant and robust system.
    4. In case the requested data is unavailable, system should be able to forward request to other nodes.
    5. System should be able to handle large files and  be able to process it fast
    6. Manage frequently requested data using cache

### Cluster Communication

  The whole project is based on communication between nodes in a cluster, between clusters, and end-user with the system. We decided to use gPRC as our mechanism for inter cluster communication. We compared RESTful+JSON and gRPC+Protobuf. gRPC+Protobuf showed better performance than REST+JSON for sample data.

### Architecture Diagram

In this project we have multi-cluster architecture like below diagram. Each cluster has different technology stack but the only common component is communication channel which is gRPC with Protobuf. All clusters agreed upon one proto file which contains structure of all methods and objects that are used for inter cluster communication.
<img src="https://github.com/soodkshitij/TEST/blob/master/src/clusters.png" /> 

We followed hub-spoke architecture and point to point communication style. Also we used Raft for leader election so once a leader is selected it becomes point of contact for external cluster or end user. Each node has MonogoDB instance on it to store all data. When we load data into our cluster we create bloom filter at leader node and also keep updating it as new data comes. We used gRPC for intra cluster communication as well. If our leader receives request for data from external client/end user, leader will serve the data and also cache last few request’s output into cache.
<img src="https://github.com/soodkshitij/TEST/blob/master/src/singleCluster.png" />

### Caching

We implemented caching using memcache, which is the high performance distributed memory object caching system. In order to decrease the database load and avoid the round trip to database, we cached the frequent requests.

### Hashing

We created the bloom filter for the set of dates existing in our cluster and used its definite true negatives property to determine if we have data existing in our cluster or if we need to forward the requests to external cluster. 
Bloom filter - A bloom filter which is the space efficient probabilistic data structure is used to test the existence of the element in the set. 

## Start up steps.

    1. Need a Network switch to connect to the network.
    2. Set up the config.cfg as with cluster node's IP's
    3. Start client.py
    4. Start server.py


## Team Members:
* Maulik Bhatt
* Kshitij Sood
* Ardalan Razavi
* Astha Kumar

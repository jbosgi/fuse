# Fabric Partition

Fabric partition provides the ability of defining a task, that can be "partiotioned" and distributed to fabric containers.
Each task can be associated with multiple work items (aka partitions) and those are distributed to the containers.


###Terminology
* **Task** The definition of the work.
* **Partition** A portion of the work (work unit) that is distributed to the container. It is represented in json format.
* **PartitionListener** An interface that describes how the container should handle when added/removed a partition. The master listens for changes in the partitions and calls the BalancingPolicy if needed. Both master and slave react to partition assignment and notify the PartitionListener.

##Creating Tasks
Fabric provides a ManagedServiceFactory that is responsible for creating a TaskManager for each configured task.
To create a new TaskManager the user needs add a configuration with the factoryPid org.fusesource.fabric.partition, e.g:

    fabric:profile-edit --resource org.fusesource.fabric.partition-example.properties <target profile>

The command above will open a text editor where the user can define the task configuration:

    id=example
    partitions.path=/fabric/task/example
    balancing.policy=even
    worker.type=camel-sping
    task.definition=profile:camel.xml

In this configuration, the id uniquely identifies the configuration.
The balancing policy describes how the partitions should be balanced. Fabric provides out of the box the "even" balancing policy, but the user can implement his own and export them as an OSGi service using the service property "type" to distinguish.
The key partitions.path defines the path in the registry where the partitions are stored.
The worker.type defines the PartitionListener implementation. The implementation is looked up in the OSGi service reference, using the property "type" as a filter.
The task.definition is defines the task. It can be any value the PartitionListener can understand. In the current example it specifies a url to camel spring xml file. The camel-spring PartitionListener knows how to handle it.


##Implementing custom partition listeners
In most cases the user will want to implement his own partition listener. Implementing one is as trivial as implementing the following methods.

    String getType();

    void start(String taskId, String taskDefinition, Set<Partition> partitions);

    void stop(String taskId, String taskDefinition, Set<Partition> partitions);

The getType() method should return a string, which can be used for looking up the listener (used in the worker.type property).
The start/stop methods can be used to implement the behavior of the listener when partition items are added/removed. The arguemnts taskId and taskDefintion take values fromt he task configuration. The partions argument is a representation of the partition, which contains a unique identifier for the partition and a java.util.Map created from the json data of the Partition stored in the registry.


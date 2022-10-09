# kafka_assignment
  * kafka assignment on restaurant_orders
## Steps:
1. Download the Restaurant Order Dataset. 
![image](https://user-images.githubusercontent.com/36133568/194768452-55fab404-211a-4df5-8352-c3d3488838f3.png)
2.Setup Confluent Kafka Account
  ![image](https://user-images.githubusercontent.com/36133568/194768480-09edfb4e-24af-46e2-827c-90defc5e8913.png)
3.Setup key (string) & value (json) schema in the confluent schema registry
   * Value Schema
  ![image](https://user-images.githubusercontent.com/36133568/194768503-3882af59-8658-4739-8a48-f0916279ddad.png)
   *  Key Schema
   ![image](https://user-images.githubusercontent.com/36133568/194768552-646f5973-bb84-4ffc-8163-db0a22f0ca64.png)
4.	Write a kafka producer program (python or any other language) to read data records from restaurent data csv file, make sure schema is not hardcoded in the producer code, read the latest version of schema and schema_str from schema registry and use it for data serialization.
 Schema is not hardcoded as can been seen in this below snippet.It is being automatically pulled from following methods:
* get_subjects() ->returns list of subjects.
* get_latest_version() -> Parameter: subject(Schema name),returns: schemaversion
* get_schema() -> Parameter : Schema _id , returns: Schema_str
Ref:https://docs.confluent.io/5.5.1/clients/confluent-kafka-python/_modules/confluent_kafka/schema_registry/schema_registry_client.html#SchemaRegistryClient
![image](https://user-images.githubusercontent.com/36133568/194768648-ff49b654-6e2b-4995-a42f-fe5590b0b655.png)
5. From producer code, publish data in Kafka Topic one by one and use dynamic key while publishing the records into the Kafka Topic
![image](https://user-images.githubusercontent.com/36133568/194768674-a4942826-3510-40f7-b80c-9bf266d9952f.png)
![image](https://user-images.githubusercontent.com/36133568/194768685-7387c16f-1e56-415e-9f67-81ff49c5d81b.png)
Output displayed in command prompt
![image](https://user-images.githubusercontent.com/36133568/194768691-7c440136-9523-485b-9923-dad634828477.png)
![image](https://user-images.githubusercontent.com/36133568/194768698-4e5a1d17-6f3e-43b5-9a2d-b70eda97eef5.png)

6. Write kafka consumer code and create two copies of same consumer code and save it with different names (kafka_consumer_1.py & kafka_consumer_2.py), again make sure lates schema version and schema_str is not hardcoded in the consumer code, read it automatically from the schema registry to desrialize the data. 
   Now test two scenarios with your consumer code:
    * Use "group.id" property in consumer config for both consumers and mention different group_ids in kafka_consumer_1.py & kafka_consumer_2.py,apply "earliest" offset property in both consumers and run these two consumers from two different terminals. Calculate how many records each consumer consumed and printed on the terminal
    Kafka_consumer_1.py
![image](https://user-images.githubusercontent.com/36133568/194768753-8ebe4c28-408e-4ca8-b053-0869cf145dc6.png)
 Kafka_consumer_2.py
 ![image](https://user-images.githubusercontent.com/36133568/194768769-817bf21e-43a7-459a-a84b-32f37793d2f0.png)
 
 Total Orders Consumed by Consumer 1: 53720
 Total Orders Consumed by Consumer 2: 49180

 Total Orders Consumed for each consumer with different group ID
 ![image](https://user-images.githubusercontent.com/36133568/194768783-d1e4f367-b868-4b51-81be-7d5d450f40dd.png)

  *  Use "group.id" property in consumer config for both consumers and mention same group_ids in kafka_consumer_1.py & kafka_consumer_2.py, apply "earliest" offset property in both consumers and run these two consumers from two different terminals. Calculate how many records each consumer consumed and printed on the terminal
![image](https://user-images.githubusercontent.com/36133568/194768838-85daebe6-65bf-45ff-9077-877e32096e7d.png)
![image](https://user-images.githubusercontent.com/36133568/194768846-b1b25052-77a6-41c2-a47e-9f64a07213b2.png)
Consumer 1 consumed: 50063
Consumer 2 consumed: 24755
![image](https://user-images.githubusercontent.com/36133568/194768863-c167f411-6cdc-403b-9efc-8af12673b036.png)

7. Once above questions are done, write another kafka consumer to read data from kafka topic and from the consumer code create one csv file "output.csv" and append consumed records output.csv file

![image](https://user-images.githubusercontent.com/36133568/194768907-a601600f-db12-460d-b70e-d7db55568eec.png)
Output CSV file Image
![image](https://user-images.githubusercontent.com/36133568/194768912-761a85cf-bc78-4915-9528-c1bac533af13.png)



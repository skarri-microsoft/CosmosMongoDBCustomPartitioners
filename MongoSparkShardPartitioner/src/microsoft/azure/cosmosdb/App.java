package microsoft.azure.cosmosdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class App {
    private static ConfigSettings configSettings = new ConfigSettings();
    private static MongoClientExtension mongoClientExtension;
    private static int MaxRetries = 10;

    public static void main(String[] args) throws Exception {


        configSettings.Init();
        InitMongoClient36();

        // ASCII Partitioner
        AsciiStringPartitioner.Init(mongoClientExtension, configSettings);

        // Shard key of the collection, estimated collection capacity to pull docs per second in cross partition queries
        HashMap<String,MinMax> asciiStringRanges=
                AsciiStringPartitioner.GetAsciiStringRanges("storeId", 2000, 50);


        // Object ID partitioner
        // Pre-creates objectid based shard information in Cosmos DB Mongo Config-chunks collection
//        ObjectIdPartitioner.Init(mongoClientExtension,configSettings);
//        ObjectIdPartitioner.GenerateIDPartitionRanges("_id",50);

    }

    private static void InitMongoClient36() {
        mongoClientExtension = new MongoClientExtension();
        mongoClientExtension.InitMongoClient36(
                configSettings.getUserName(),
                configSettings.getPassword(),
                10255,
                true,
                configSettings.getClientThreadsCount()
        );
    }
}

package microsoft.azure.cosmosdb;

import com.google.common.collect.Iterables;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.Iterator;

public class ObjectIdPartitioner {

    private static MongoClientExtension mongoClientExtension;
    private static ConfigSettings configSettings;

    public static void Init(
            MongoClientExtension mongoClientExtension,
            ConfigSettings configSettings) {
        ObjectIdPartitioner.mongoClientExtension = mongoClientExtension;
        ObjectIdPartitioner.configSettings = configSettings;
    }

    public static void GenerateIDPartitionRanges(String shardKey, int partitionsCount) {
        // Setup is mandatory
        SetupConfigDb(mongoClientExtension.GetClient());
        Bson minDoc = BsonDocument.parse("{" + shardKey + ":1}");
        Bson maxDoc = BsonDocument.parse("{" + shardKey + ":-1}");
        ObjectId minId =
                Iterables.get(mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                        .getCollection(configSettings.getCollName()).find().sort(minDoc).limit(1), 0).getObjectId("_id");

        ObjectId maxId =
                Iterables.get(mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                        .getCollection(configSettings.getCollName()).find().sort(maxDoc).limit(1), 0).getObjectId("_id");

        PartitionUtil.GenerateIdRanges(
                minId,
                maxId,
                shardKey,
                mongoClientExtension.GetClient(),
                "config",
                "chunks",
                configSettings.getDbName(),
                configSettings.getCollName(),
                partitionsCount);

    }

    private static void SetupConfigDb(MongoClient client) {
        String collectionName = "chunks";
        MongoDatabase database = client.getDatabase("config");
        MongoIterable<String> collections = database.listCollectionNames();
        Iterator<String> it = collections.iterator();
        while (it.hasNext()) {
            if (it.next() == collectionName) {
                return;
            }
        }
        database.getCollection(collectionName).createIndex(new Document("min", 1));
        database.getCollection(collectionName).createIndex(new Document("max", 1));
        database.getCollection(collectionName).createIndex(new Document("shard", 1));

    }
}

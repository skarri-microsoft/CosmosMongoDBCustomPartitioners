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

    private static HashMap<String, MinMax> ReArrangeBoundries(HashMap<String, MinMax> statRanges) {
        HashMap<String, MinMax> rearranged = new HashMap<>();
        List<String> keys = new ArrayList<>(statRanges.keySet());
        Collections.sort(keys);
        for (int i = 0; i < keys.size() - 1; i++) {
            MinMax current = statRanges.get(keys.get(i));
            MinMax next = statRanges.get(keys.get(i + 1));
            rearranged.put(current.Min, current);
            next.Min = current.Max;
            rearranged.put(next.Min, next);
        }
        // Last one raise the max Condition

        MinMax last = statRanges.get(keys.get((keys.size() - 1)));
        last.Max = "" + ((char) (last.Max.charAt(0) + 1));

        return rearranged;
    }


    private static HashMap<String, MinMax> SortAndMergeFinalRangeStats(
            HashMap<String, MinMax> finalStatRanges,
            long partitionSize) {
        if (finalStatRanges.size() < 2) {
            return finalStatRanges;
        }

        HashMap<String, MinMax> statRanges = ReArrangeBoundries(finalStatRanges);
        HashMap<String, MinMax> mergesRangesByPartitionSize = new HashMap<>();
        List<String> keys = new ArrayList<>(statRanges.keySet());
        Collections.sort(keys);
        long count = 0;
        MinMax merged = null;
        boolean isMerged = false;
        for (int i = 0; i < keys.size(); i++) {
            MinMax current = statRanges.get(keys.get(i));

            if (count == 0) {
                merged = current;
            }

            // If this crosses partition size don't include
            if (count + current.Count > partitionSize) {
                if (count == 0) {
                    mergesRangesByPartitionSize.put(current.Min, current);
                    isMerged = true;
                } else {
                    merged.Max = statRanges.get(keys.get(i - 1)).Max;
                    merged.Count = count;
                    mergesRangesByPartitionSize.put(merged.Min, merged);
                    isMerged = true;
                }
            } else {
                count = count + current.Count;
            }

            if (isMerged && count != 0) {
                merged = current;
                count = current.Count;
                isMerged = false;
            }

        }

        // Last element
        if (!isMerged) {
            merged.Max = statRanges.get(keys.get(keys.size() - 1)).Max;
            merged.Count = count;
            mergesRangesByPartitionSize.put(merged.Min, merged);
        }
        return mergesRangesByPartitionSize;
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

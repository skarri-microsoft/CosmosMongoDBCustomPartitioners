package microsoft.azure.cosmosdb;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;

public class AsciiStringPartitioner {
        private static MongoClientExtension mongoClientExtension;
        private static ConfigSettings configSettings;

    public static void Init(
            MongoClientExtension mongoClientExtension,
            ConfigSettings configSettings) {
        AsciiStringPartitioner.mongoClientExtension = mongoClientExtension;
        AsciiStringPartitioner.configSettings = configSettings;
    }

    public static HashMap<String,MinMax> GetAsciiStringRanges(
            String key,
            long collectionCapacityToPullTheDocuments,
            long batchSize) throws Exception {

        long TotalDocuments=GetCount(); // Total documents
        long parallelTasks=collectionCapacityToPullTheDocuments/batchSize; // in a way we need to these amy partitions

        // While building the partition, the range should have enough documents
        // Make sure each range can pull these many documents
        long partitionSize=TotalDocuments/parallelTasks;

        HashMap<String,MinMax> FinalRangeStats=new HashMap<>();

        Queue<MinMax> queue = new LinkedList<MinMax>();
        queue.add(getMinMax(null,key));

        while(!queue.isEmpty())
        {
            MinMax element=queue.poll();
            HashMap<String,String> ranges=PartitionUtil.StringRanges(
                    element.Min,
                    element.Max
            );

            //Optimization, can't split any more irrespective of the partition size
            if(ranges.size()==1)
            {
                FinalRangeStats.put(element.Min,element);
            }
            else
            {
                HashMap<String,MinMax> statRanges= getRangeStats(ranges,key);
                Iterator it = statRanges.entrySet().iterator();

                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry)it.next();
                    if(((MinMax)pair.getValue()).Count>partitionSize)
                    {
                        MinMax filter=((MinMax)pair.getValue());

                        MinMax check=getMinMax(filter,key);
                        if(check.Min.equals(check.Max))
                        {
                            FinalRangeStats.put(pair.getKey().toString(),((MinMax)pair.getValue()));
                        }
                        else
                        {
                            queue.add(getMinMax(filter, key));
                        }
                    }
                    else
                    {
                        FinalRangeStats.put(pair.getKey().toString(),((MinMax)pair.getValue()));
                    }
                }}
        }

        HashMap<String,MinMax> RearrangedFinalStats= ReArrangeBoundries(FinalRangeStats);
        TreeMap<String, MinMax> updated= UpdateFinalRangeStats(RearrangedFinalStats,key);

        System.out.println("Printing final calculated ranges.....keys count: "+FinalRangeStats.size());
        PrintFinalRangeStats(updated);

        System.out.println("Sorting and Merging final calculated ranges.....");

        HashMap<String,MinMax> sortAndMergedRanges=
                SortAndMergeFinalRangeStats(GetHashMap(updated),partitionSize);

        System.out.println("Validating Sorted and Merged ranges.....keys Count: "+sortAndMergedRanges.size());
        ValidateFinalSortedRange(sortAndMergedRanges,key);
        return sortAndMergedRanges;
    }
    private static void  ValidateFinalSortedRange(HashMap<String,MinMax> rangeStatsInput, String key) {

        MongoCollection<Document> collection=mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                .getCollection(configSettings.getCollName());
        TreeMap<String,MinMax> rangeStats=new TreeMap<>();
        rangeStats.putAll(rangeStatsInput);
        Iterator it = rangeStats.entrySet().iterator();
        long totalCount=0;
        while (it.hasNext()) {

            Map.Entry pair = (Map.Entry)it.next();
            MinMax minMax=(MinMax) pair.getValue();
            List<BasicDBObject> criteria = new ArrayList<BasicDBObject>();
            BasicDBObject minFilter=new BasicDBObject(key,new BasicDBObject("$gte",minMax.Min));
            BasicDBObject maxFilter=new BasicDBObject(key,new BasicDBObject("$lt",minMax.Max));
            criteria.add((minFilter));
            criteria.add(maxFilter);

            long count=collection.count(new BasicDBObject("$and", criteria));

            System.out.println("Min: "+minMax.Min+" Max: "+minMax.Max+" Expected: "+minMax.Count+" Actual:"+count+" isEqual: "+(minMax.Count==count));

            totalCount=totalCount+count;
        }

        System.out.println("Total number of documents : "+ totalCount);

    }
    private static long GetCount() {
        MongoCollection<Document> collection = mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                .getCollection(configSettings.getCollName());
        return collection.count();
    }

    private static HashMap<String, MinMax> GetHashMap(TreeMap<String, MinMax> updated) {
        HashMap<String, MinMax> hashMap = new HashMap<>();
        Iterator it = updated.entrySet().iterator();
        long totalCount = 0;
        while (it.hasNext()) {

            Map.Entry pair = (Map.Entry) it.next();
            MinMax minMax = (MinMax) pair.getValue();
            hashMap.put(pair.getKey().toString(), minMax);
        }
        return hashMap;
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

    private static TreeMap<String, MinMax> UpdateFinalRangeStats(HashMap<String, MinMax> statRangesInput, String shardKey) {
        TreeMap<String, MinMax> statRanges = new TreeMap<>(statRangesInput);
        String[] keys = statRanges.keySet().toArray(new String[statRanges.size()]);
        Arrays.sort(keys);

        for (int i = 0; i < keys.length - 1; i++) {
            statRanges.put(
                    keys[i],
                    getUpdatedMinMax(statRanges.get(keys[i]), shardKey));

        }

        // Last element
        statRanges.put(
                keys[keys.length - 1],
                getUpdatedMinMax(statRanges.get(keys[keys.length - 1]), shardKey));


        return statRanges;
    }

    private static void PrintFinalRangeStats(TreeMap<String, MinMax> statRanges) {
        Iterator it = statRanges.entrySet().iterator();
        long totalCount = 0;
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            MinMax item = ((MinMax) pair.getValue());
            totalCount = totalCount + item.Count;
            System.out.println("Min :" + item.Min + " Max: " + item.Max + " Count: " + item.Count);
        }
        System.out.println("Total number of documents: " + totalCount);
    }

    // Makes server call
    private static MinMax getUpdatedMinMax(
            MinMax item,
            String key) {
        MongoCollection<Document> collection = mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                .getCollection(configSettings.getCollName());
        List<BasicDBObject> criteria = new ArrayList<BasicDBObject>();
        BasicDBObject minFilter = new BasicDBObject(key, new BasicDBObject("$gte", item.Min));
        BasicDBObject maxFilter = new BasicDBObject(key, new BasicDBObject("$lt", item.Max));

        criteria.add((minFilter));
        criteria.add(maxFilter);

        item.Count = collection.count(new BasicDBObject("$and", criteria));
        return item;
    }

    private static MinMax getMinMax(MinMax filter, String key) {
        MongoCollection<Document> collection = mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                .getCollection(configSettings.getCollName());

        MinMax target = new MinMax();
        Bson minDoc = BsonDocument.parse("{" + key + ":1}");
        Bson maxDoc = BsonDocument.parse("{" + key + ":-1}");
        if (filter != null) {
            List<BasicDBObject> criteria = new ArrayList<BasicDBObject>();
            BasicDBObject minFilter = new BasicDBObject(key, new BasicDBObject("$gte", filter.Min));
            BasicDBObject maxFilter = new BasicDBObject(key, new BasicDBObject("$lt", filter.Max));
            criteria.add(minFilter);
            criteria.add(maxFilter);
            Document findFilter = new Document("$and", criteria);
            target.Min = collection.find(findFilter).sort(minDoc).limit(1).first().getString(key);
            target.Max = collection.find(findFilter).sort(maxDoc).limit(1).first().getString(key);
        } else {
            target.Min = collection.find().sort(minDoc).limit(1).first().getString(key);
            target.Max = collection.find().sort(maxDoc).limit(1).first().getString(key);
        }

        return target;
    }

    private static HashMap<String, MinMax> getRangeStats(
            HashMap<String, String> ranges,
            String key) {

        MongoCollection<Document> collection = mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                .getCollection(configSettings.getCollName());

        HashMap<String, MinMax> rangeStats = new HashMap<>();
        Iterator it = ranges.entrySet().iterator();
        long totalCount = 0;
        while (it.hasNext()) {
            MinMax minMax = new MinMax();
            Map.Entry pair = (Map.Entry) it.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());
            minMax.Min = pair.getKey().toString();
            minMax.Max = pair.getValue().toString();
            List<BasicDBObject> criteria = new ArrayList<BasicDBObject>();
            BasicDBObject minFilter = new BasicDBObject(key, new BasicDBObject("$gte", pair.getKey()));
            BasicDBObject maxFilter = new BasicDBObject(key, new BasicDBObject("$lt", pair.getValue()));
            criteria.add((minFilter));
            criteria.add(maxFilter);

            long count = collection.count(new BasicDBObject("$and", criteria));
            minMax.Count = count;
            rangeStats.put(minMax.Min, minMax);
            totalCount = totalCount + count;

        }
        return rangeStats;
    }
}

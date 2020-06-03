package microsoft.azure.cosmosdb;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

// Specs
// https://docs.mongodb.com/manual/tutorial/create-chunks-in-sharded-cluster/

public class PartitionUtil {

    public static int FindCommonPrefixPosition(String min,String max)
    {
        int indexPos=0;
        while(true)
        {

            // It enters into this when both keys are not equal
            if(min.length()<=indexPos || max.length()<=indexPos)
            {
                return indexPos-1;
            }
            if(min.charAt(indexPos)==max.charAt(indexPos))
            {
                indexPos++;
            }
            else {
                return indexPos;
            }
        }
    }

    public static HashMap<String,String> StringRanges(String min,String max)
    {
        HashMap<String,String> map=new HashMap<>();
        if(min.equals(max))
        {
            map.put(min,max);
            return map;
        }
        int prefixPos=FindCommonPrefixPosition(min,max);

        int startingPrefix=min.charAt(prefixPos)+1;
        int endingPrefix=max.charAt(prefixPos)+1;
        String commonPrefix=min.substring(0,prefixPos);
        String minPrefix=min;
        while(startingPrefix<=endingPrefix)
        {
                String maxPrefix=commonPrefix+(char)startingPrefix;
                if(maxPrefix.compareTo(max)>0)
                {
                    map.put(minPrefix, max);
                    return map;
                }
                else {
                    map.put(minPrefix, maxPrefix);
                    minPrefix = maxPrefix;
                    startingPrefix++;
                }

                //Optimization
                // If you reach the max then don't add it, just return the processed ones
                if(minPrefix.equals(max))
                {
                    return map;
                }

        }
        return map;
    }

    public static void GenerateIdRanges(
            ObjectId min,
            ObjectId max,
            String shardKey,
            MongoClient mongoClient,
            String configDbName,
            String chunkCollName,
            String sourceDbName,
            String sourceCollectionName,
            int numOfPartitions) {

        Date lowerBoundDate = SubtractHours(min.getDate(), 1);
        Date upperBoundDate = AddHours(max.getDate(), 1);

        long upperBound = upperBoundDate.getTime();
        long lowerBound = lowerBoundDate.getTime();

        long interval =
                ((upperBound - lowerBound) / numOfPartitions) +
                        ((upperBound - lowerBound) % numOfPartitions);


        //dummy value but must be incremental
        int shardId = 1;

        // Just apply one extra range to cover upper bound
        // In spark driver lower bound is inclusive and upper bound is exclusive
        while (lowerBound < (upperBound + interval)) {
            Document configObj = new Document("ns", sourceDbName + "." + sourceCollectionName)
                    .append("min", new BasicDBObject(shardKey, new ObjectId(new Date(lowerBound))))
                    .append("max", new BasicDBObject(shardKey, new ObjectId(new Date((lowerBound + interval)))))
                    .append("shard", Integer.toString(shardId));
            mongoClient.getDatabase(configDbName).getCollection(chunkCollName).insertOne(configObj);

            lowerBound = lowerBound + interval;
            shardId++;
        }
    }

    private static Date AddHours(Date date, int hours) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.HOUR, hours);
        return cal.getTime();
    }

    private static Date SubtractHours(Date date, int hours) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.HOUR, -hours);
        return cal.getTime();
    }

}

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class AirportTaxiReducer extends Reducer<Text, IntWritable, Text, Text> {
    private Map<String, Double> resultMap;

    @Override
    protected void setup(Context context) {
        resultMap = new HashMap<>();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int count = 0, sum = 0;
        for(IntWritable value : values) {
            sum += value.get();
            count++;
        }
        if(sum > 0 && count > 0) {
            Double avgTaxiTime = (double) (sum) / (double) (count);
            resultMap.put(key.toString(), avgTaxiTime);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<String, Double> shortestTaxi;
        Map<String, Double> longestTaxi;

        shortestTaxi = resultMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (v1, v2) -> v1, LinkedHashMap::new));

        longestTaxi = resultMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (v1, v2) -> v1, LinkedHashMap::new));

        int counter = 0;
        context.write(new Text("Total airport entries"), new Text(String.valueOf(longestTaxi.entrySet().size())));
        context.write(new Text("Airports with Longest Average Taxi Time"), new Text("Average Time in Minutes"));
        for(Map.Entry<String, Double> entry : longestTaxi.entrySet()) {
            BigDecimal bd = BigDecimal.valueOf(entry.getValue());
            bd = bd.setScale(4, RoundingMode.HALF_UP);
            String value = String.valueOf(bd.doubleValue());
            context.write(new Text(entry.getKey()), new Text(value));
            counter++;
            if(counter == 3) break;
        }

        counter = 0;
        context.write(new Text("Airports with Shortest Average Taxi Time"), new Text("Average Time in Minutes"));
        for(Map.Entry<String, Double> entry : shortestTaxi.entrySet()) {
            BigDecimal bd = BigDecimal.valueOf(entry.getValue());
            bd = bd.setScale(4, RoundingMode.HALF_UP);
            String value = String.valueOf(bd.doubleValue());
            context.write(new Text(entry.getKey()), new Text(value));
            counter++;
            if(counter == 3) break;
        }
    }
}
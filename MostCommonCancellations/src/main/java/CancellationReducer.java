import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class CancellationReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    Map<String, Long> resultMap;

    @Override
    protected void setup(Context context) {
        resultMap = new HashMap<>();
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) {
        String keyValue = key.toString().trim();
        long count = 0L;
        for(LongWritable value : values) {
            count += value.get();
        }
        resultMap.put(keyValue, count);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<String, Long> result;

        result = resultMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (v1, v2) -> v1, LinkedHashMap::new));

        for(Map.Entry<String, Long> entry : result.entrySet()) {
            Text outputKey = new Text(entry.getKey());
            LongWritable outputValue = new LongWritable(entry.getValue());
            context.write(outputKey, outputValue);
        }
    }
}
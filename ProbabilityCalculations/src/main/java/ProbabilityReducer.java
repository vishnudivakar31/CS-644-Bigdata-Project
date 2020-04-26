import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public  class ProbabilityReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private Map<String, Double> result;

    @Override
    protected void setup(Context context) {
        result = new HashMap<>();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {

        int total_count = 0;
        int on_time = 0;

        for(IntWritable value : values) {
            if(value.get()== 1){
                on_time++;
            }
            total_count++;
        }

        double probability =  (double)on_time / (double)total_count;
        result.put(key.toString(), probability);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        LinkedHashMap<String, Double> sortedMap = new LinkedHashMap<>();

        result.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));

        context.write(new Text("The 3 Airlines with Lowest Probability - "),new DoubleWritable(3));
        int count=0;
        for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
            String airline = entry.getKey();
            Double probability = entry.getValue();
            System.out.println(airline);
            System.out.println(probability);
            context.write(new Text(airline), new DoubleWritable(probability));
            count++;
            if(count==3){
                break;
            }
        }

        LinkedHashMap<String, Double> reverseSortedMap = new LinkedHashMap<>();

        result.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));

        context.write(new Text("The Airlines with Highest Probability - "),new DoubleWritable(3));
        count=0;
        for (Map.Entry<String, Double> entry : reverseSortedMap.entrySet()) {
            String airline = entry.getKey();
            Double probability = entry.getValue();
            System.out.println(airline);
            System.out.println(probability);
            context.write(new Text(airline), new DoubleWritable(probability));
            count++;
            if(count==3){
                break;
            }
        }
    }
}

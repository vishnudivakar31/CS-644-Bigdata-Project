package edu.njit.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class TopCancellationReasons {

    private enum CancellationCodes {
            A,
            B,
            C,
            D
    }

    private static final String[] cancellationReason = new String[] {
            "Carrier Related Reasons",
            "Weather Related Reasons",
            "National Airspace System Related Reasons",
            "Security Related Reasons"
    };

    private static final Integer CANCELLATION_CODE_INDEX = 22;

    public static class CancellationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] rawData = value.toString().split(",");
            String cancellationCode = rawData[CANCELLATION_CODE_INDEX].trim();
            LongWritable one = new LongWritable(1);
            if(cancellationCode.length() > 0) {
                CancellationCodes code = CancellationCodes.valueOf(cancellationCode);
                Text outputKey = new Text(cancellationReason[code.ordinal()]);
                context.write(outputKey, one);
            }
        }
    }

    public static class CancellationReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
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
            Map<String, Long> result = new HashMap<>();
            
            resultMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue())
                    .forEachOrdered(item -> result.put(item.getKey(), item.getValue()));

            for(Map.Entry<String, Long> entry : result.entrySet()) {
                Text outputKey = new Text(entry.getKey());
                LongWritable outputValue = new LongWritable(entry.getValue());
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        if(args.length < 2) {
            System.out.println("Expecting input file and output directory path");
            System.exit(-1);
        }
        Job job = Job.getInstance(configuration, "Top Cancellation Reasons");
        job.setJarByClass(TopCancellationReasons.class);

        job.setMapperClass(CancellationMapper.class);
        job.setReducerClass(CancellationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

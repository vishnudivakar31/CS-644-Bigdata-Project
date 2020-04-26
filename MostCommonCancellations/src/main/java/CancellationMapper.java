import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CancellationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
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
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] rawData = value.toString().split(",");
        try {
            if(!rawData[0].equalsIgnoreCase("Year")) {
                String cancellationCode = rawData[CANCELLATION_CODE_INDEX].trim();
                LongWritable one = new LongWritable(1);
                if(cancellationCode.length() > 0) {
                    CancellationCodes code = CancellationCodes.valueOf(cancellationCode);
                    Text outputKey = new Text(cancellationReason[code.ordinal()]);
                    context.write(outputKey, one);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.StringTokenizer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class TwitterMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    String array_twitter [] = value.toString().split(";");

        
    if(array_twitter.length ==4 && array_twitter[2].length()<=140) {

    try {

    long epochTime = Long.parseLong(array_twitter[0]);

    LocalDateTime twitter_date = LocalDateTime.ofEpochSecond(epochTime/1000, 0, ZoneOffset.UTC);

    IntWritable hr = new IntWritable(twitter_date.getHour());

    context.write(hr, one);

    } 
        
    catch (NumberFormatException ignored) {

      }


        }
    }
}

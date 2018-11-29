import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class TwitterMapper extends Mapper<Object, Text, Text, IntWritable> {

  private final IntWritable one = new IntWritable(1);
    
  private Text data = new Text();

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {



    String array_twitter [] = value.toString().split(";");

    
      if(array_twitter.length ==4 && array_twitter[2].length()<=140) {

    String pattern = "#[a-zA-Z][a-zA-Z0-9_]+";

    
          try {

    long epochTime = Long.parseLong(array_twitter[0]);

    LocalDateTime twitter_date = LocalDateTime.ofEpochSecond(epochTime/1000, 0, ZoneOffset.UTC);

    int hr = twitter_date.getHour();

    
              if(hr==1) {


    Pattern r = Pattern.compile(pattern);
    Matcher m = r.matcher(array_twitter[2]);
    while (m.find()) {
    data.set(m.group(0));
    context.write(data, one);

     }

     }
              
     }         
          
    catch(NumberFormatException e) {}
           
       }

    }

}

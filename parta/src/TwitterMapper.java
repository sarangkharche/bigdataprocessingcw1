import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.Math;

public class TwitterMapper extends Mapper<Object, Text, Text, IntWritable> {
    
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {



    String clean = value.toString().replaceAll(";","");

    String array_twitter [] = value.toString().split(";");

    if(array_twitter.length >=4 && array_twitter[2].length()>0 && array_twitter[2].length()<=140) {


    int leng = (int)Math.ceil((double)array_twitter[2].length() / 5);

    int bin = leng*5 ;
    int min = (leng*5)-4;


      data.set("range" + min +"-"+bin);

      context.write(data, one);

        }
    }
}

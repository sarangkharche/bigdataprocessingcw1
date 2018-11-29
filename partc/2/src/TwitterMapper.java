import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.URI;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;


  public class TwitterMapper extends Mapper<Object, Text, Text, IntWritable> {

  private Hashtable<String, String> variable1;

  private final IntWritable one = new IntWritable(1);
  
  private Text data = new Text();

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {



      try {

    String array_twitter [] = value.toString().split(";");

    if(array_twitter.length >= 4){

      Set<String> keys = variable1.keySet();

        for(String keyzz: keys){

            if(array_twitter[2].contains(keyzz)){

              String sports = variable1.get(keyzz);

                data.set(sports);


                context.write(data, one);


    }
  }
}


    } 
      catch(NumberFormatException e) {
      System.err.println("NumberFormatException: " + e.getMessage());
}

}
@Override
protected void setup(Context context) throws IOException, InterruptedException {

  variable1 = new Hashtable<String, String>();


  URI fileUri = context.getCacheFiles()[0];

  FileSystem fs = FileSystem.get(context.getConfiguration());
  FSDataInputStream in = fs.open(new Path(fileUri));

  BufferedReader br = new BufferedReader(new InputStreamReader(in));

  String line = null;
  
    
    try {

    br.readLine();

    while ((line = br.readLine()) != null) {





      String[] fields = line.split(",");

      if (fields.length >= 5)
        variable1.put(fields[1], fields[7]);

   }
    br.close();
        
  } 
    catch (IOException e1) {
  }

    super.setup(context);
   }

}

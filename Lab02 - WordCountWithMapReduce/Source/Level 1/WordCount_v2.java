package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;

// Sử dụng lớp Configuration để truy cập các đối số dòng lệnh tại thời gian chạy
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class WordCount extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(WordCount.class);
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

  // Tạo một biến cho cài đặt phân biệt chữ hoa chữ thường trong lớp Map.
    private boolean caseSensitive = false;

    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
  
  // Tạo phương thức setup. Ta sẽ khởi tạo một đối tượng Configuration 
  // Sau đó đặt biến caseSensitive thành giá trị của biến hệ thống wordcount.case.sensitive được đặt từ dòng lệnh. Nếu không đặt giá trị thì giá trị mặc định là false.
    protected void setup(Mapper.Context context)
      throws IOException,
        InterruptedException {
      Configuration config = context.getConfiguration();
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
    }

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
   // Tùy chỉnh tắt phân biệt chữ hoa chữ thường tại đây. 
   // Nếu caseSensitive là false, toàn bộ dòng được chuyển đổi thành chữ thường trước khi được phân tích cú pháp.
      if (!caseSensitive) {
   // Sử dụng phương thức replaceAll dựa trên regex để thay thế các kí tự không phải chữ và số bằng khoảng trắng.
	      line = line.replaceAll("'",""); // Loại bỏ dấu nháy đơn (ví dụ như can't)
        line = line.replaceAll("[^a-zA-Z0-9 ]", " ");   // Thay thế các dấu câu với khoảng trắng
   // Chuyển đổi chữ hoa thành chữ thường
        line = line.toLowerCase();
      }
      Text currentWord = new Text();
        for (String word : WORD_BOUNDARY.split(line)) {
          if (word.isEmpty()) {
            continue;
          }
          currentWord = new Text(word);
          context.write(currentWord,one);
        }         
      }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }
}

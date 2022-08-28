package org.myorg;

// Import các lớp để xử lý các file input output và thiết lập hỗ trợ
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Import FileSplit để xử lý một phần của tệp input thay vì toàn bộ tệp cùng một lúc
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// Import StringUtils để làm việc với chuỗi.
// Với lớp này, ta có thể dùng phương thức stringifyException() để chuyển bất kỳ IOException nào thành một chuỗi trước khi chuyển nó đến phương thức System.error.println()
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

  // Thêm file có tên ở sau '-skip' trong tham số dòng lệnh vào distributed cache. 
  // Distributed cache sẽ được sử dụng để phân phối các dữ liệu cần thiết cho công việc
  // Ở đây distributed cache cho phép người dùng chỉ định các mẫu từ để bỏ qua trong khi đếm.
    for (int i = 0; i < args.length; i += 1) {
      if ("-skip".equals(args[i])) {
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
        i += 1;
        job.addCacheFile(new Path(args[i]).toUri());
        LOG.info("Added file to the distributed cache: " + args[i]);
      }
    }
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);

  // Lúc này, ta sẽ thêm lớp combiner vào cấu hình công việc.
  // Combiner được chạy trên mỗi mappers để xử lý thông tin cục bộ trước khi được gửi đến reducer.
  // Đối với chương trình này, cài đặt lớp Reduce như là một combiner sẽ đếm cục bộ các khóa. 
  // Ví dụ, gửi <word, 1> và <word,1> đến reducer thì combiner sẽ kết hợp chúng thành <word,2> trước khi chuyển kết quả đến reducer. 
  // Kết hợp các giá trị trước khi truyền có thể tiết kiệm rất nhiều băng thông và thời gian truyền
    job.setCombinerClass(Reduce.class);

    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private boolean caseSensitive = false;
    private long numRecords = 0;

  // Thêm biến để lưu trữ một đoạn dữ liệu đầu vào từ 1 tệp được chia nhỏ
    private String input;

  // Tạo một set các chuỗi có tên là patternToSkip. Đây là danh sách các dấu câu và các từ thừa cần được loại bỏ khỏi kết quả cuối cùng
    private Set<String> patternsToSkip = new HashSet<String>();
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    protected void setup(Mapper.Context context)
        throws IOException,
        InterruptedException {

  // Chuyển đổi thông tin từ nguồn tách thành một chuỗi để xử lý
      if (context.getInputSplit() instanceof FileSplit) {
        this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
      } else {
        this.input = context.getInputSplit().toString();
      }
      Configuration config = context.getConfiguration();
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);

  // Nếu biến wordcont.skip.patterns là true, lấy danh sách các mẫu cần bỏ qua từ tệp distributed cache và chuyển tiếp URI của tệp tới phương thức parseSkipFile
      if (config.getBoolean("wordcount.skip.patterns", false)) {
        URI[] localPaths = context.getCacheFiles();
        parseSkipFile(localPaths[0]);
      }
    }
  
  // Lấy distributed cache từ HDFS tại URI cục bộ. Đọc mỗi lần 1 dòng cho đến khi đọc hết các dòng. Thêm từng dòng vào tập hợp các chuỗi để bỏ qua.
    private void parseSkipFile(URI patternsURI) {
      LOG.info("Added file to the distributed cache: " + patternsURI);
      try {
        BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
        String pattern;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + patternsURI + "' : " + StringUtils.stringifyException(ioe));
      }
    }

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      if (!caseSensitive) {
	      line = line.replaceAll("'","");
        line = line.replaceAll("[^a-zA-Z0-9 ]", " "); 
        line = line.toLowerCase();
      }
      Text currentWord = new Text();
      for (String word : WORD_BOUNDARY.split(line)) {
   // Sửa đổi câu lệnh if để nếu biến word trống hoặc nó chứa một trong các mẫu đã được xác định cần bỏ qua, 
  //  vòng lặp for sẽ tiếp tục mà không ghi giá trị vào biến context
        if (word.isEmpty() || patternsToSkip.contains(word)) {
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


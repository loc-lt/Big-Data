// Sử dụng package tương ứng với domain
package org.myorg;

// Sử dụng 2 thư viện chuẩn của Java là IOException và regex.pattern (trích xuất các từ từ file input)
import java.io.IOException;
import java.util.regex.Pattern;

// Mở rộng lớp Configured và áp dụng lớp Tools-> cho Hadoop biết chương trình được chạy trong một đối tượng configuration.
// Sau đó, sử dụng ToolRunner để chạy ứng dụng MapReduce
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Lớp Job sẽ tạo, điều chỉnh và chạy một phiên bản của chương trình MapReduce. 
// Ta mở rộng lớp Mapper  và thêm các hướng dẫn xử lý để tạo thành lớp Map. Điều tương tự diễn ra với Reducer: mở rộng nó để tạo và tùy chỉnh lớp Reduce riêng.
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// Sử dụng lớp Path để truy cập file trong HDFS. Trong các hướng dẫn cấu hình công việc, ta chuyển các đường dẫn cần thiết bằng các lớp FileInputFormat và FileOutputFormat
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Ta sẽ sử dụng các lớp như StringWritable vì nó thực hiện các chức năng ghi, đọc và so sánh các giá trị trong quá trình map-reduce với các đối tượng số nguyên (IntWritable) và số nguyên dài (LongWritable)
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// Lớp Logger gửi các thông báo sửa lỗi từ bên trong các lớp mapper và reducer. Khi chạy ứng dụng, một trong những thông báo INFO tiêu chuẩn cung cấp một URL mà bạn có thể sử dụng để theo dõi sự hoàn thành của công việc
// Thông báo chuyển cho Logger sẽ được hiển thị trong nhật ký (file log) của map và reduce cho công việc trên máy chủ Hadoop
import org.apache.log4j.Logger;

// Lớp WordCount bao gồm phương thức main và run cùng các lớp bên trong là Map và Reduce. Lớp này bắt đầu bằng cách khởi tạo trình ghi nhật ký (Logger)
public class WordCount extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(WordCount.class);

  // Phương thức main sẽ gọi ToolRunner, công cụ này tạo và chạy một phiên bản WordCount mới, truyền các đối số dòng lệnh. 
  // Khi chương trình kết thúc, nó trả về một giá trị số nguyên cho trạng thái, giá trị này được chuyển đến đối tượng System khi thoát
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res);
  }
  // Phương thức run sẽ cấu hình công việc (bao gồm thiết lập các đường dẫn được truyền vào trong dòng lệnh), bắt đầu công việc, đợi công việc hoàn thành và sau đó trả về một giá trị số nguyên dưới dạng flag (thành công/thất bại)
  public int run(String[] args) throws Exception {
    //Tạo 1 đối tượng Job mới. Ta dùng phương thức Configured.getConf() để lấy đối tượng cấu hình cho phiên bản WordCount này và đặt tên cho đối tượng công việc là wordcount
    Job job = Job.getInstance(getConf(), "wordcount");

    // Chỉ định file jar chứa trình điều khiển, mapper và reducer. Hadoop sẽ chuyển tệp jar này đến các nút trong cụm đang chạy các tác vụ map và reduce.
    job.setJarByClass(this.getClass());
    
    // Thiết lập đường dẫn input và output cho chương trình.
    // Lưu trữ file input trong HDFS sau đó chuyển các đường dẫn ở trên dưới dạng đối số dòng lệnh 
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    // Thiết lập lớp Map và Reduce cho công việc, ta dùng các lớp Map và Reduce được định nghĩa trong lớp WordCount này
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    
    // Xác định kiểu output cho cả Map và Reduce
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Chạy công việc và chờ nó hoàn thành (phương thức WaitForCompletion(boolean)).
    // Nếu công việc chạy thành công thì trả về 0. Ngược lại thì trả về 1.
    return job.waitForCompletion(true) ? 0 : 1;
  }
  // Lớp Map (mở rộng từ Mapper) biến đổi các giá trị key/value thành các cặp key/value trung gian để gửi đến Reducer.
  // Lớp này nhận 4 tham số: loại dữ liệu của input key, input value, loại dữ liệu của output key, output value (chính là loại dữ liệu của input key, input value cho reducer)
  // Lớp sẽ định nghĩa vài biến toàn cục, bắt đầu bằng một InrWritable cho giá trị 1, một đối tượng Text được dùng để lưu trữ từng từ khi nó được phân tích từ chuỗi đầu vào
  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

  // Tạo  một mẫu biểu thức chính quy (regular expression) để phân tích cú pháp từng dòng văn bản đầu vào trên các ranh giới từ ("\ b"). 
  // Các ranh giới từ bao gồm khoảng trắng, tab và các dấu câu.
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

  // Gọi phương thức map 1 lần cho mỗi cặp key/value từ nguồn đầu vào. 
  // Phương thức này nhận đầu vào một offset kiểu LongWritable, lineText kiểu Text và một đối tượng Context
  // Lúc này, phương thức map nhận offset (phần bù) của ký tự đầu tiên trong dòng input hiện tại dưới dạng key. 
  // Nó tiếp tục phân tích cú pháp các từ trong dòng để tạo output trung gian
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {

  // Chuyển một dòng ở dạng đối tượng Text thành một chuỗi. Tạo biến currentWord để lưu từng từ từ mỗi chuỗi input
      String line = lineText.toString();
      Text currentWord = new Text();

  // Sử dụng biểu thức chính quy để chia dòng thành các từ riêng biệt dựa trên ranh giới từ đã được định nghĩa ở trên. 
  // Nếu một đối tượng từ rỗng (ví dụ như bao gồm khoảng trắng), chuyển đến đối tượng được phân tích tiếp theo.
  // Nếu không, ghi một cặp khóa/giá trị vào đối tượng context.
      for (String word : WORD_BOUNDARY.split(line)) {
        if (word.isEmpty()) {
            continue;
        }
            currentWord = new Text(word);
            context.write(currentWord,one);
        }
    }
  }
  
  // Lớp mapper tạo một cặp key/value cho mỗi từ bao gồm từ và giá trị IntWritable 1.
  // Lớp Reducer xử lý từng cặp, thêm một cho từ hiện tại trong cặp key/value vào tổng số lần xuất hiện của từ đó từ tất cả mappers.
  // Sau đó, nó ghi kết quả cho từ đó vào đối tượng context của reducer và chuyển sang phần tiếp theo.
  // Lớp Reducer yêu cầu 4 tham số: loại dữ liệu của input key, input value (là loại dữ liệu của output key, output value từ mapper), loại dữ liệu của output key, output value
  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
  // Phương thức reduce chạy một lần với mỗi key nhận được từ pha shuffle and sort của MapReduce
  // Phương thức nhận đầu vào một word kiểu Text, tập counts kiểu IntWritable và một đối tượng Context
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;

  // Với mỗi giá trị trong tập giá trị được chuyển đến bởi mapper:  Thêm giá trị vào bộ đếm số lượng từ cho word (từ) tương ứng
      for (IntWritable count : counts) {
        sum += count.get();
      }
  // Khi mà mọi cặp key/value trung gian đã được xử lý, tác vụ map-reduce hoàn tất. Chương trình lưu kết quả vào vị trí output trong HDFS
      context.write(word, new IntWritable(sum));
    }
  }
}

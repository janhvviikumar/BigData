package wordcount;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountCompleteShakespeare extends Configured implements Tool {

   
    // 1. MAPPER CLASS (StopWordMapper)
   
    public static class StopWordMapper extends 
        Mapper<LongWritable, Text, Text, IntWritable> {

        private Text outputKey = new Text();
        private final static IntWritable ONE = new IntWritable(1);
        
        // **HARDCODED STOP WORD LIST 
        private static final Set<String> STOP_WORDS = new HashSet<String>(
            Arrays.asList(new String[]{
                "i", "me", "my", "myself", "we", "our", "ours", "ourselves", 
                "you", "your", "yours", "yourself", "yourselves", "he", "him", 
                "his", "himself", "she", "her", "hers", "herself", "it", "its", 
                "itself", "they", "them", "their", "theirs", "themselves", "what", 
                "which", "who", "whom", "this", "that", "these", "those", "am", 
                "is", "are", "was", "were", "be", "been", "being", "have", "has", 
                "had", "having", "do", "does", "did", "doing", "a", "an", "the", 
                "and", "but", "if", "or", "because", "as", "until", "while", "of", 
                "at", "by", "for", "with", "about", "against", "between", "into", 
                "through", "during", "before", "after", "above", "below", "to", 
                "from", "up", "down", "in", "out", "on", "off", "over", "under", 
                "again", "further", "then", "once", "here", "there", "when", 
                "where", "why", "how", "all", "any", "both", "each", "few", 
                "more", "most", "other", "some", "such", "nonor", "not", "only", 
                "own", "same", "so", "than", "too", "very", "can", "will", "just", 
                "don't", "should", "now"
            })
        );

       
        @Override
        protected void setup(Context context) throws IOException,
                                         InterruptedException {
            System.out.println("-----Inside setup()-----");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                                         throws IOException, InterruptedException {
            System.out.println("-----Inside map()-----");
            
            // Split the line into words
            String[] words = StringUtils.split(value.toString(), '\\', ' ');
            
            for(String word: words) {
                // 1. Clean the word--remove all non-alphabetic characters and lowercase it
                String cleanedWord = word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim();
                
                // 2. Filter--Check if the word is NOT empty AND is NOT in the STOP_WORDS set
                if (!cleanedWord.isEmpty() && !STOP_WORDS.contains(cleanedWord)) {
                
                    outputKey.set(cleanedWord); // Use the clean, lowercase word as the key
                    context.write(outputKey, ONE); //Send this as output for shuffle/sort
                }
            }
        }
    }

    
    // 2. REDUCER CLASS (StopWordReducer)
  
    public static class StopWordReducer extends 
        Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                                      throws IOException, InterruptedException {
            int sum = 0;
            // Sum up the counts for the word (key)
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result); //count of each word is sent as output
        }
    }

  
    // 3. RUN METHOD (Job Configuration)
    
    @Override
    public int run(String[] args) throws Exception {
        
        // Validate arguments: requires <input path> and <output path>
        if (args.length != 2) {
            System.err.println("Usage: WordCountCompleteShakespeare <input path> <output path>");
            return 1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "WordCountCompleteShakespeare");
        
        // as the stop words are hardcoded.

        job.setJarByClass(getClass());
        
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        
        // Delete the output folder if it exists (Ensures a clean run)
        FileSystem fs = out.getFileSystem(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }
        
        // Configure I/O Paths
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        // Configure Classes
        job.setMapperClass(StopWordMapper.class);
        job.setReducerClass(StopWordReducer.class); 
        
        // Configure Output Formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Configure Key/Value Types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Set number of Reducers to 3 (as required by the problem statement)
        job.setNumReduceTasks(3); 

        // Submit the job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    
    // 4. MAIN METHOD
 
    public static void main(String[] args) {
        int result = 0;
        try {
            result = ToolRunner.run(new Configuration(),
                                    new WordCountCompleteShakespeare(),
                                    args);
        } catch (Exception e) {
            e.printStackTrace();
            result = 1;
        }
        System.exit(result);
    }
}

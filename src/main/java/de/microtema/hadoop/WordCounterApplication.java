package de.microtema.hadoop;

import java.io.IOException;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import javax.swing.plaf.synth.SynthOptionPaneUI;

public class WordCounterApplication {

    private final JobConf jobConf;

    public WordCounterApplication(String inputPaths, String outputPath) {
        Validate.notEmpty(inputPaths, "inputPaths are required");
        Validate.notEmpty(outputPath, "outputPath is required");

        jobConf = new JobConf(getClass());

        jobConf.setJobName(getClass().getSimpleName());

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapperClass(WordCounterMapper.class);
        jobConf.setCombinerClass(WordCounterMapReducer.class);
        jobConf.setReducerClass(WordCounterMapReducer.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf, new Path(inputPaths));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
    }

    public void run() throws IOException {

        RunningJob runningJob = JobClient.runJob(jobConf);

        JobID jobID = runningJob.getID();

        String jobName = runningJob.getJobName();

        System.out.println(String.format("WordCounterApplication.run with job-ID: %s and name: %s", jobID.getId(), jobName));
    }


    public static void main(String[] args) throws IOException {

        new WordCounterApplication(args[0], args[1]).run();
    }
}

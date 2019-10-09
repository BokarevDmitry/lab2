import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import java.nio.

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AirportApp {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: AirportApp <input> <input> <output>");
            System.exit(-1);
        }

        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(confi); // получаем конфигурацию
        // Осторожно! Как-никак удаляем директорию - вдруг там что полезное =)
        if (hdfs.exists(outputFile)) { // если существует,
            hdfs.delete(output, true); // то удаляем
        }

        Job job = Job.getInstance();
        job.setJarByClass(AirportApp.class);
        job.setJobName("AirportApp");

        job.setReducerClass(ReduceJoiner.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AirportMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FlightMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
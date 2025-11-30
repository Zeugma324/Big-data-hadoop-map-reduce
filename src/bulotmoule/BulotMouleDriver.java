package bulotmoule;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BulotMouleDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: BulotMouleDriver <input_dir> <output_dir>");
            System.exit(-1);
        }

        String inputBase = args[0];
        String outputBase = args[1];

        Path pProduit = new Path(inputBase + "/Produit.csv");
        Path pConcerner = new Path(inputBase + "/Concerner.csv");
        Path pMagasin = new Path(inputBase + "/Magasin.csv");
        Path pCommande = new Path(inputBase + "/Commande.csv");

        Path tempProd = new Path(outputBase + "_temp_produits");
        Path tempMag = new Path(outputBase + "_temp_magasins");
        Path tempFilter = new Path(outputBase + "_temp_filter");
        Path finalOutput = new Path(outputBase);

        FileSystem fs = FileSystem.get(getConf());
        fs.delete(tempProd, true);
        fs.delete(tempMag, true);
        fs.delete(tempFilter, true);
        fs.delete(finalOutput, true);

        Job job1 = Job.getInstance(getConf(), "Job1_Produit_Concerner");
        job1.setJarByClass(BulotMouleDriver.class);

        MultipleInputs.addInputPath(job1, pProduit, TextInputFormat.class, ProduitMapper.class);
        MultipleInputs.addInputPath(job1, pConcerner, TextInputFormat.class, ConcernerMapper.class);

        job1.setReducerClass(ProduitConcernerReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job1, tempProd);


        Job job2 = Job.getInstance(getConf(), "Job2_Magasin_Commande");
        job2.setJarByClass(BulotMouleDriver.class);

        MultipleInputs.addInputPath(job2, pMagasin, TextInputFormat.class, MagasinMapper.class);
        MultipleInputs.addInputPath(job2, pCommande, TextInputFormat.class, CommandeMapper.class);

        job2.setReducerClass(MagasinCommandeReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job2, tempMag);


        Job job3 = Job.getInstance(getConf(), "Job3_Filter_MoulesBulots");
        job3.setJarByClass(BulotMouleDriver.class);

        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setMapperClass(FinalFilterMapper.class); 
        job3.setReducerClass(FinalFilterReducer.class);

        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job3, tempProd);
        FileOutputFormat.setOutputPath(job3, tempFilter);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);


        Job job4 = Job.getInstance(getConf(), "Job4_Final_Merge");
        job4.setJarByClass(BulotMouleDriver.class);

        MultipleInputs.addInputPath(job4, tempFilter, KeyValueTextInputFormat.class, FilterResultMapper.class);

        MultipleInputs.addInputPath(job4, tempMag, KeyValueTextInputFormat.class, InverseMagasinMapper.class);

        job4.setReducerClass(FinalDisplayReducer.class);
        
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        
        FileOutputFormat.setOutputPath(job4, finalOutput);


        ControlledJob cJob1 = new ControlledJob(getConf());
        cJob1.setJob(job1);
        
        ControlledJob cJob2 = new ControlledJob(getConf());
        cJob2.setJob(job2);
        
        ControlledJob cJob3 = new ControlledJob(getConf());
        cJob3.setJob(job3);
        
        ControlledJob cJob4 = new ControlledJob(getConf());
        cJob4.setJob(job4);

        cJob3.addDependingJob(cJob1);

        cJob4.addDependingJob(cJob3);
        cJob4.addDependingJob(cJob2);


        JobControl jobControl = new JobControl("BulotMouleWorkflow");
        jobControl.addJob(cJob1);
        jobControl.addJob(cJob2);
        jobControl.addJob(cJob3);
        jobControl.addJob(cJob4);

        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        while (!jobControl.allFinished()) {
            System.out.println("Running... " 
                + " Waiting: " + jobControl.getWaitingJobList().size()
                + " Ready: " + jobControl.getReadyJobsList().size()
                + " Running: " + jobControl.getRunningJobList().size()
                + " Success: " + jobControl.getSuccessfulJobList().size());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        return jobControl.getFailedJobList().isEmpty() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BulotMouleDriver(), args);
        System.exit(exitCode);
    }
}
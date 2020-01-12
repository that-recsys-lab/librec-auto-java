package net.that_recsys_lab.auto.cmd;

import net.librec.common.LibrecException;
import net.librec.conf.Configuration;
import net.librec.data.convertor.TextDataConvertor;
import net.librec.util.DriverClassUtil;
import net.that_recsys_lab.auto.AutoRecommenderJob;
import net.that_recsys_lab.auto.IJobCmd;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;

public class CheckCmd implements IJobCmd {
    private AutoRecommenderJob job;

    public CheckCmd(AutoRecommenderJob job) {
        this.job = job;
    }

    private void checkPath(String path) throws FileNotFoundException, NotDirectoryException {
        // in case it is a path that's relative to another path property
        File pathObj = new File(job.getConf().get(path));

        if (!pathObj.exists()) {
            throw new FileNotFoundException(path + " " + pathObj + " does not exist.");
        }
        else if (!pathObj.isDirectory()) {
            throw new NotDirectoryException(path + " " + pathObj + " exists but is not a directory.");
        }
    }

    public void execute() throws LibrecException, IOException, ClassNotFoundException {
        job.getLOG().info("CheckCmd: START");

        // Extract classes and paths
        String recommenderClass = job.getConf().get("rec.recommender.class");
//        job.getLOG().info("conf " + recommenderClass);
        String similarityClass = job.getConf().get("rec.similarity.class");
//        job.getLOG().info("conf " + similarityClass);
        // evaluator class
        String evaluatorClass = job.getConf().get("rec.eval.classes");

        // required
        job.getLOG().info(DriverClassUtil.getClass(recommenderClass));
        job.getLOG().info(DriverClassUtil.getClass(similarityClass));
        job.getLOG().info(DriverClassUtil.getClass(evaluatorClass));

        // required
        checkPath("dfs.data.dir");
        checkPath("dfs.result.dir");
        checkPath("dfs.log.dir");
        File dataInputPath = new File(job.getConf().get("dfs.data.dir") + "/" + job.getConf().get("data.input.path"));
        if (!dataInputPath.exists()) {
            throw new FileNotFoundException(dataInputPath + " does not exist.");
        }
        else if (dataInputPath.isDirectory() && dataInputPath.list().length == 0) {
            throw new IOException(dataInputPath + " is an empty directory.");
        }
        checkPath("data.input.path");


        job.getLOG().info("CheckCmd: FINISH");
    }

    private Configuration getConf() {
        return job.getConf();
    }
}

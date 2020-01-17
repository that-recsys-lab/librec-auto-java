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

    private void checkPath(String pathkey) {
        // in case it is a path that's relative to another path property
        File pathObj = new File(job.getConf().get(pathkey));

        if (!pathObj.exists()) {
            job.getLOG().info("Error: "  + pathkey + " " + pathObj + " does not exist.");
        }
        else if (!pathObj.isDirectory()) {
            job.getLOG().info("Error: " + pathkey + " " + pathObj + " exists but is not a directory.");
        }
    }

    private void checkClasses(String classeskey) {
        String[] asClass = job.getConf().getStrings(classeskey);
        for (int i = 0; i < asClass.length; i++) {
            checkClass1(classeskey, asClass[i]);
        }
    }

    private void checkClass(String classkey) {
        String sClass = job.getConf().get(classkey);
        //job.getLOG().info("Checking class: " + classkey + " " + sClass);
        checkClass1(classkey, sClass);
    }

    private void checkClass1(String classkey, String sClass) {
        if (sClass == null) {
            job.getLOG().info("Warning: " + classkey + " is null.");
            return;
        } else {
            try {
                DriverClassUtil.getClass(sClass);
            } catch (ClassNotFoundException e) {
                job.getLOG().info("Error: " + classkey + " " + sClass + " does not exist.");
            }
        }
    }

    public void execute() throws LibrecException, IOException, ClassNotFoundException {
        job.getLOG().info("CheckCmd: START");

        // Check classes and paths
        checkClass("rec.recommender.class");
        checkClass("rec.similarity.class");
        checkClasses("rec.eval.classes");

        // required
        checkPath("dfs.data.dir");
        checkPath("dfs.result.dir");
        checkPath("dfs.log.dir");
        File dataInputPath = new File(job.getConf().get("dfs.data.dir") + "/" + job.getConf().get("data.input.path"));
        if (!dataInputPath.exists()) {
            job.getLOG().info("Error: Data input path " + dataInputPath + " does not exist.");
        }
        else if (dataInputPath.isDirectory() && dataInputPath.list().length == 0) {
            job.getLOG().info("Error: Data input path " + dataInputPath + " exists but is empty.");
        }
        //checkPath("data.input.path");

        job.getLOG().info("CheckCmd: FINISH");
    }

    private Configuration getConf() {
        return job.getConf();
    }
}

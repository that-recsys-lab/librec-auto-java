package net.that_recsys_lab.auto.cmd;

import com.google.common.collect.BiMap;
import net.librec.math.structure.SequentialAccessSparseMatrix;
import net.librec.math.structure.SequentialSparseVector;
import net.that_recsys_lab.auto.AutoRecommenderJob;
import net.that_recsys_lab.auto.IJobCmd;
import net.librec.common.LibrecException;
import net.librec.conf.Configuration;
import net.librec.math.structure.SequentialAccessSparseMatrix;
import net.librec.math.structure.SequentialAccessSparseMatrix;
import net.librec.math.structure.Vector.VectorEntry;
import net.librec.util.FileUtil;

import java.io.IOException;

public class SplitCmd implements IJobCmd {
    private AutoRecommenderJob job;
    private int m_splitId;
    private boolean m_saveToFile = true;
    private boolean m_kcvReload = false;

    // C'tor
    public SplitCmd(AutoRecommenderJob job) {
        this.job = job;
    }
    public SplitCmd(AutoRecommenderJob job, int splitId){
        this.job = job;
        this.m_splitId = splitId;
    }
    public SplitCmd(AutoRecommenderJob job, int splitId, boolean saveToFile, boolean kcvreload){
        this.job = job;
        this.m_splitId = splitId;
        this.m_saveToFile = saveToFile;
        this.m_kcvReload = kcvreload;
    }
    private Double splitRatio;

    // Interface interaction
    public void execute() throws LibrecException {
        try {
            if(m_kcvReload){
                if(this.m_splitId > 1) {
                    job.getConf().setBoolean("data.convert.read.ready",false);
                    job.getConf().set("data.model.splitter", "testset");
                    job.getConf().set("data.input.path", "split/cv_"+this.m_splitId+"/train.txt");
                    job.getConf().set("data.testset.path", "split/cv_"+this.m_splitId+"/test.txt");
                    job.setData();
                }
            }
            else {
                splitRatio = Double.parseDouble(getConf().get("data.splitter.trainset.ratio")); //This is set-up
                SplitData();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * ~ Auto-Method ~
     *
     * Generate data model by fold and save.
     *
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws LibrecException
     */
    private void SplitData() throws LibrecException, IOException, ClassNotFoundException {
        job.getLOG().info("SplitCMD: START - Splitting training and testing.");
        if(job.m_data.hasNextFold()) {
            job.m_data.nextFold();
            if(m_saveToFile) {
                getConf().set("data.splitter.cv.index", String.valueOf(this.m_splitId));
                SaveGivenSplitData(this.m_splitId);
            }
        }
    }

    /**
     * ~ Auto-Method ~
     *
     * Save train and test data.
     *
     * @throws LibrecException        if error occurs
     * @throws IOException            if I/O error occurs
     * @throws ClassNotFoundException if class not found error occurs
     */

    private void SaveGivenSplitData(int CurrentFold) throws LibrecException, IOException, ClassNotFoundException {
        job.getLOG().info("SplitCMD: Splitting training and testing with "+splitRatio.toString()+"% ratio on fold "+CurrentFold+".");

        //  Set-Up  //
        job.getData().buildDataModel();

        SequentialAccessSparseMatrix test = genTestMatrix();
        SequentialAccessSparseMatrix train = genTrainMatrix();

        int numUsersTest = test.rowSize();
        int numUsersTrain = train.rowSize();

        BiMap<String, Integer> userMapping = job.getData().getUserMappingData(); // rather than //m_data
        BiMap<String, Integer> itemMapping = job.getData().getItemMappingData(); // rather than //m_data

        BiMap<Integer, String> userMappingInverse = userMapping.inverse();
        BiMap<Integer, String> itemMappingInverse = itemMapping.inverse();

        // TRAIN //
        StringBuilder train_out = new StringBuilder();
        for (int i_uid = 0; i_uid < numUsersTrain; i_uid++) {
            SequentialSparseVector row_i = train.row(i_uid);
            String userId = userMappingInverse.get(i_uid);
            for (VectorEntry i: row_i){
                String itemId = itemMappingInverse.get(i.index());
                double rating = i.get();
                train_out.append(userId).append("\t").append(itemId).append("\t").append(rating).append("\n");
            }
        }

        //  Write TrainResultData
        String saveDataTrain = train_out.toString();
        try {
            String outputPathTrain = trainFileNameGen();
            FileUtil.writeString(outputPathTrain, saveDataTrain);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // TEST //
        StringBuilder test_out = new StringBuilder();
        for (int i_uid = 0; i_uid < numUsersTest; i_uid++) {
            SequentialSparseVector row_i = test.row(i_uid);
            String userId = userMappingInverse.get(i_uid);
            for (VectorEntry i: row_i){
                String itemId = itemMappingInverse.get(i.index());
                double rating = i.get();
                test_out.append(userId).append("\t").append(itemId).append("\t").append(rating).append("\n");
            }
        }

        // Write TestResultData
        String saveDataTest = test_out.toString();
        try {
            String outputPathTest = testFileNameGen();
            FileUtil.writeString(outputPathTest, saveDataTest);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**                              ~  Auto Aux functions  ~
     * Helper functions:
     *
     * getConf()                          -> Retrieves properties file from invoker.
     * genTrainMatrix()/genTestMatrix()   -> Facade for GenMatrixAux() for ease and readability of use.
     * genMatrixAux()                     -> Creates sparse matrix of training or test set.
     * trainFileNameGen()/testFileNameGen -> Facade for fileNameGenAux() for ease and readability of use.
     * fileNameGenAux()                   -> Creates file name string based on train/test and split (defaults to 1 if not cv.)
     *
     */

    private Configuration getConf() {
        return job.getConf();
    }
    private SequentialAccessSparseMatrix genTrainMatrix(){ return genMatrixAux(true);}
    private SequentialAccessSparseMatrix genTestMatrix(){ return genMatrixAux(false);}

    private SequentialAccessSparseMatrix genMatrixAux(boolean i){
        SequentialAccessSparseMatrix ret;
        if(i){ ret = job.getData().getDataSplitter().getTrainData(); }
        else { ret = job.getData().getDataSplitter().getTestData(); }
        return ret;
    }

    private String trainFileNameGen(){ return fileNameGenAux(true); }
    private String testFileNameGen(){ return fileNameGenAux(false); }

    // Generate files paths
    private String fileNameGenAux(Boolean flag){
        String outputPath = getConf().get("dfs.data.dir")+'/'+getConf().get("dfs.split.dir");
        //Had to move "Split" dir into "Data" dir in order to maintain how LibRec searches for Data in directory...
        if (flag) {
            outputPath = outputPath+"/cv_"+ getConf().get("data.splitter.cv.index", "1")+"/train.txt";
        }
        else {
            outputPath = outputPath+"/cv_"+getConf().get("data.splitter.cv.index", "1")+"/test.txt";
        }
        return outputPath;
    }
}

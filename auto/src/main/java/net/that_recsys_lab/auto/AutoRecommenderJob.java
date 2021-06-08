package net.that_recsys_lab.auto;

import com.google.common.collect.BiMap;
import net.librec.math.structure.SequentialAccessSparseMatrix;
import net.librec.math.structure.SequentialSparseVector;
import net.librec.math.structure.Vector;
import net.librec.util.DriverClassUtil;
import net.librec.util.FileUtil;
import net.that_recsys_lab.auto.cmd.*;

import net.librec.common.LibrecException;
import net.librec.data.DataSplitter;
import net.librec.data.splitter.KCVDataSplitter;
import net.librec.data.splitter.LOOCVDataSplitter;
import net.librec.eval.EvalContext;
import net.librec.eval.Measure;
import net.librec.eval.RecommenderEvaluator;
import net.librec.filter.RecommendedFilter;
import net.librec.math.algorithm.Randoms;
import net.librec.math.structure.DataSet;
import net.librec.math.structure.SymmMatrix;
import net.librec.recommender.Recommender;
import net.librec.conf.Configuration;
import net.librec.data.DataModel;
import net.librec.recommender.RecommenderContext;
import net.librec.recommender.item.RecommendedItem;
import net.librec.similarity.RecommenderSimilarity;
import net.librec.util.JobUtil;
import org.apache.commons.logging.Log;
import net.librec.util.ReflectionUtil;

import java.io.*;
import java.util.*;

/*
 @librec-auto
 @DePaul-WIL
 @Aldo-OG
 @Masoud Mansoury
 */

// INVOKER
public class AutoRecommenderJob extends net.librec.job.RecommenderJob{
    /*  Attributes  */
    private List<IJobCmd> m_commands;  // instantiate each component in 'setCommands()'
    protected Configuration m_conf;
    public DataModel m_data;
    protected String m_modelSplit;
    protected int m_cvCount; // loocv not supported
    protected boolean m_kcvReload;
    public Map<String, List<Double>> m_cvEvalResults;
    public List<Recommender> m_recommenders;
    private Map<Measure.MeasureValue, Double> m_evaluatedMap;

    public AutoRecommenderJob(Configuration conf) {
        super(conf);
        this.m_conf = conf;
        this.m_modelSplit = conf.get("data.model.splitter");  // Error handle this
		m_cvCount = 1;
		this.m_cvEvalResults = new HashMap<>();
        if (m_modelSplit.equals("kcv")) { // or 'locv'
            m_cvCount = m_conf.getInt("data.splitter.cv.number", 1);
            m_kcvReload = m_conf.getBoolean("data.splitter.cv.reload", false);
            if(m_kcvReload){
                m_conf.set("data.model.splitter","testset");
                m_conf.set("data.input.path", "split/cv_1/train.txt");
                m_conf.set("data.testset.path", "split/cv_1/test.txt");
            }
        } else {
            
        }
        this.m_recommenders = new ArrayList<>(m_cvCount);
        try {
            setData();
        } catch (Exception e) {  // IOException, LibrecException, or Other...
            e.printStackTrace();
        }
    }

    /**                              ~  Auto Aux functions  ~
     * Helper functions:
     *
     * getConf()                          -> For CMD use. Retrieves properties file from invoker.
     * getLOG()                           -> For CMD use. Retrieves LOG object for writing.
     * getCVCount()                       -> For CMD use. Retrieves number of folds in CV, defaults to 1.
     * getData()                          -> For CMD use. Retrieves DataModel.
     *
     */
    public Configuration getConf() { return m_conf; }
    public Log getLOG() { return LOG; }
    public int getCVCount() { return m_cvCount; }
    public DataModel getData(){ return this.m_data; }

    /**
     * Assigned reference to m_data and builds model
     *
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws LibrecException
     */
    public void setData()throws ClassNotFoundException, IOException, LibrecException {
        m_data = ReflectionUtil.newInstance((Class<DataModel>) this.getDataModelClass(), m_conf);
        m_conf.set("data.splitter.cv.index", "1"); // done in case of a CV load.  Will crash on buildDataModel otherwise.
        m_data.buildDataModel();
    }

    /**
     *  Initialize commands based on input option:
     *     split: Split only
     *     exp: Experiment only, assumes prior split
     *     eval: Evaluation only, assumes prior experiment
     *     **full: split, exp, eval.
     */
    public void setCommands(String arg) {
        this.m_commands = new ArrayList<IJobCmd>(5);
        List<IJobCmd> cmds;

        switch (arg) {
            case "check":
                this.m_commands.add(new CheckCmd(this));
                break;
            case "split":
//                    this.m_commands.add(new SplitCmd(this));
                cmds = new ArrayList<IJobCmd>(m_cvCount);
                for (int i = 1; i <= m_cvCount; i++) {
                    List<IJobCmd> subcmds = new ArrayList<IJobCmd>();
                    subcmds.add(new SplitCmd(this, i));
                    cmds.add(new SeqCmd(this, subcmds));
                }
                this.m_commands.add(new SeqCmd(this, cmds));
                break;
            case "reRunEval":
//                for (int i = 1; i <= m_cvCount; i++) {
//                    this.m_commands.add(new ReRunEvalCmd(this, i));
//                }
                cmds = new ArrayList<IJobCmd>(m_cvCount);
                for (int i = 1; i <= m_cvCount; i++) {
                    List<IJobCmd> subcmds = new ArrayList<IJobCmd>();
                    subcmds.add(new SplitCmd(this, i, false, true));
                    subcmds.add(new ReRunEvalCmd(this, i));
                    cmds.add(new SeqCmd(this, subcmds));
                }
                this.m_commands.add(new SeqCmd(this, cmds));
                break;
            case "exp-eval":
                cmds = new ArrayList<IJobCmd>(m_cvCount);
                for (int i = 1; i <= m_cvCount; i++) {
                    List<IJobCmd> subcmds = new ArrayList<IJobCmd>();
                    subcmds.add(new SplitCmd(this, i, false, true));
                    subcmds.add(new ExpCmd(this, i));
                    subcmds.add(new EvalCmd(this, i));
                    cmds.add(new SeqCmd(this, subcmds));
                }
                this.m_commands.add(new SeqCmd(this, cmds));
                break;
            case "run":
                cmds = new ArrayList<IJobCmd>(m_cvCount);
                for (int i = 1; i <= m_cvCount; i++) {
                    List<IJobCmd> subcmds = new ArrayList<IJobCmd>();
                    subcmds.add(new SplitCmd(this, i, true, false));
                    subcmds.add(new SplitCmd(this, i, false, true));
                    subcmds.add(new ExpCmd(this, i));
                    subcmds.add(new EvalCmd(this, i));
                    cmds.add(new SeqCmd(this, subcmds));
                }
                this.m_commands.add(new SeqCmd(this, cmds));
                break;
            case "full":
//                this.m_commands.add(new SplitCmd(this));
                cmds = new ArrayList<IJobCmd>(m_cvCount);
                for (int i = 1; i <= m_cvCount; i++) {
                    List<IJobCmd> subcmds = new ArrayList<IJobCmd>();
                    subcmds.add(new SplitCmd(this, i, false, true));
                    subcmds.add(new ExpCmd(this, i));
                    subcmds.add(new EvalCmd(this, i));
                    cmds.add(new SeqCmd(this, subcmds));
                }
                this.m_commands.add(new SeqCmd(this, cmds));
                break;

            default: {
                System.out.println("You've failed to enter a proper command.  LibRecAuto Terminating.");
                return;
            }
        }
    }

    /**
     * ~ Auto-Method ~
     * Run's job in manner similar to status quo LibRec, but with CMD pattern.
     *
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws LibrecException
     */
    @Override
    public void runJob() throws LibrecException, IOException, ClassNotFoundException {
        for (IJobCmd cmd : this.m_commands) {
            cmd.execute();
        }
        if(m_cvCount > 1){printCVAverageResult();}
    }

    /***
     *
     *
     *
     *                            * * * Minor changes to LibRec private methods below * * *
     *
     *
     *
     ***/

    /**
     *  *Auto Changed: The function name to avoid overloading issues and accessibility to public.
     *
     * Generate similarity.
     *
     *
     * @param context recommender context
     */

    public void generateSimilarityAutoOverload(RecommenderContext context) {
        String[] similarityKeys = m_conf.getStrings("rec.recommender.similarities");
        if (similarityKeys != null && similarityKeys.length > 0) {
            for(int i = 0; i< similarityKeys.length; i++){
                if (getSimilarityClass(i) != null) {
                    RecommenderSimilarity similarity = (RecommenderSimilarity) ReflectionUtil.newInstance(getSimilarityClass(i), m_conf);
                    m_conf.set("rec.recommender.similarity.key", similarityKeys[i]);

                    similarity.buildSimilarityMatrix(m_data);
                    if(similarityKeys[i].equals("item") || similarityKeys[i].equals("user")){
                            context.setSimilarity(similarity);
                    }
                    context.addSimilarities(similarityKeys[i], similarity);
                }
            }
        }
    }

    /**
     *  *Auto Changed: The function name to avoid overloading issues and accessibility to public.
     *
     * Execute evaluator.
     *
     * @param recommender  recommender algorithm
     * @throws LibrecException        if error occurs
     * @throws IOException            if I/O error occurs
     * @throws ClassNotFoundException if class not found error occurs
     */
    public void executeEvaluatorAutoOverload(Recommender recommender, RecommenderContext context) throws ClassNotFoundException, IOException, LibrecException {
        if (m_conf.getBoolean("rec.eval.enable")) {
            DataSet dataSet =  m_data.getTestDataSet();
            String[] similarityKeys = m_conf.getStrings("rec.recommender.similarities");
            EvalContext evalContext = null;
            if (similarityKeys != null && similarityKeys.length > 0) {
                if(context.getSimilarity() == null)
                    this.generateSimilarityAutoOverload(context);
                SymmMatrix similarityMatrix = context.getSimilarity().getSimilarityMatrix();
                Map<String, RecommenderSimilarity> similarities = context.getSimilarities();
                evalContext = new EvalContext(m_conf, recommender, dataSet, similarityMatrix, similarities);
            } else {
                evalContext = new EvalContext(m_conf, recommender, dataSet);
            }


            String[] evalClassKeys = m_conf.getStrings("rec.eval.classes");
            if (evalClassKeys != null && evalClassKeys.length > 0) {// Run the evaluator which is
                // designated.
                for (int classIdx = 0; classIdx < evalClassKeys.length; ++classIdx) {
                    RecommenderEvaluator evaluator = ReflectionUtil.newInstance(getEvaluatorClass(evalClassKeys[classIdx]), null);
                    evaluator.setTopN(m_conf.getInt("rec.recommender.ranking.topn", 10));
//                    evaluator.setDataModel(m_data);

                    double evalValue = evaluator.evaluate(evalContext);
                    LOG.info("Evaluator info:" + evaluator.getClass().getSimpleName() + " is " + evalValue);
                    collectCVResults(evaluator.getClass().getSimpleName(), evalValue);
                }
            } else {// Run all evaluators
                m_evaluatedMap = new HashMap<>();
                boolean isRanking = m_conf.getBoolean("rec.recommender.isranking");
                int topN = 10;
                if (isRanking) {
                    topN = m_conf.getInt("rec.recommender.ranking.topn", 10);
                    if (topN <= 0) {
                        throw new IndexOutOfBoundsException("rec.recommender.ranking.topn should be more than 0!");
                    }
                }
                List<Measure.MeasureValue> measureValueList = Measure.getMeasureEnumList(isRanking, topN);
                if (measureValueList != null) {
                    for (Measure.MeasureValue measureValue : measureValueList) {
                        RecommenderEvaluator evaluator = ReflectionUtil
                                .newInstance(measureValue.getMeasure().getEvaluatorClass());
                        if (isRanking && measureValue.getTopN() != null && measureValue.getTopN() > 0) {
                            evaluator.setTopN(measureValue.getTopN());
                        }
                        double evaluatedValue = evaluator.evaluate(evalContext);
                        m_evaluatedMap.put(measureValue, evaluatedValue);
                    }
                }
                if (m_evaluatedMap.size() > 0) {
                    for (Map.Entry<Measure.MeasureValue, Double> entry : m_evaluatedMap.entrySet()) {
                        String evalName = null;
                        if (entry != null && entry.getKey() != null) {
                            if (entry.getKey().getTopN() != null && entry.getKey().getTopN() > 0) {
                                LOG.info("Evaluator value:" + entry.getKey().getMeasure() + " top " + entry.getKey().getTopN() + " is " + entry.getValue());
                                evalName = entry.getKey().getMeasure() + " top " + entry.getKey().getTopN();
                            } else {
                                LOG.info("Evaluator value:" + entry.getKey().getMeasure() + " is " + entry.getValue());
                                evalName = entry.getKey().getMeasure() + "";
                            }
                            if (null != m_cvEvalResults) {
                                collectCVResults(evalName, entry.getValue());
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     *  *Auto Changed: first conditional to not fail if splitter value is changed due to use of testset.
     *
     * Collect the evaluate results when using cross validation.
     *
     * @param evalName   name of the evaluator
     * @param evalValue  value of the evaluate result
     */
    private void collectCVResults(String evalName, Double evalValue) {
        DataSplitter splitter = m_data.getDataSplitter();
        if (splitter != null) { // && (splitter instanceof KCVDataSplitter || splitter instanceof LOOCVDataSplitter)) {
            if (m_cvEvalResults.containsKey(evalName)) {
                m_cvEvalResults.get(evalName).add(evalValue);
            } else {
                List<Double> newList = new ArrayList<>();
                newList.add(evalValue);
                m_cvEvalResults.put(evalName, newList);
            }
        }
    }

    /***
     *
     *
     *
     *                            * * * Direct copy of LibRec private methods below * * *
     *
     *
     *
     ***/

    /**
     * Filter the results.
     *
     * @param recommendedList  list of recommended items
     * @return recommended List
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private List<RecommendedItem> filterResult(List<RecommendedItem> recommendedList) throws ClassNotFoundException, IOException {
        if (getFilterClass() != null) {
            RecommendedFilter filter = (RecommendedFilter) ReflectionUtil.newInstance(getFilterClass(), null);
            recommendedList = filter.filter(recommendedList);
        }
        return recommendedList;
    }

    private void printCVAverageResult() {
        DataSplitter splitter = m_data.getDataSplitter();
        if (splitter != null && (splitter instanceof KCVDataSplitter || splitter instanceof LOOCVDataSplitter)) {
            LOG.info("Average Evaluation Result of Cross Validation:");
            for (Map.Entry<String, List<Double>> entry : m_cvEvalResults.entrySet()) {
                String evalName = entry.getKey();
                List<Double> evalList = entry.getValue();
                double sum = 0.0;
                for (double value : evalList) {
                    sum += value;
                }
                double avgEvalResult = sum / evalList.size();
                LOG.info("Evaluator value:" + evalName + " is " + avgEvalResult);
            }
        }
    }

    // Should be removed
//    public void SaveSplittedData(SequentialAccessSparseMatrix data, int foldNum, String dataType){
//        StringBuilder sb = new StringBuilder();
//        BiMap<String, Integer> userMapping = getData().getUserMappingData();
//        BiMap<String, Integer> itemMapping = getData().getItemMappingData();
//        if (userMapping != null && userMapping.size() > 0 && itemMapping != null && itemMapping.size() > 0) {
//            BiMap<Integer, String> userMappingInverse = userMapping.inverse();
//            BiMap<Integer, String> itemMappingInverse = itemMapping.inverse();
//            for(int u = 0; u < data.rowSize(); u++){
//                SequentialSparseVector u_recs = data.row(u);
////                for(int j = 0; j < u_recs.size(); j++) {
////                    Vector.VectorEntry q = u_recs.getVectorEntry(0);
//                for(Vector.VectorEntry entry : u_recs){
//                    String userId = userMappingInverse.get(u);
//                    String itemId = itemMappingInverse.get(entry.index());
//                    sb.append(userId).append("\t").append(itemId).append("\t").append(String.valueOf(entry.get())).append("\n");
//                }
//
////                }
//            }
//            String rawData = sb.toString();
//            // save resultData
//            try {
//                FileUtil.writeString("Results/"+dataType+"-"+String.valueOf(foldNum), rawData);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }

}

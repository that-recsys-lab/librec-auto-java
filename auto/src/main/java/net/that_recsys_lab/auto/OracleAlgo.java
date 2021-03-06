package net.that_recsys_lab.auto;

import net.librec.data.structure.*;
import com.google.common.collect.BiMap;
import net.librec.common.LibrecException;
import net.librec.data.convertor.TextDataConvertor;
import net.librec.math.structure.SequentialAccessSparseMatrix;
import net.librec.recommender.AbstractRecommender;
import net.librec.recommender.MatrixRecommender;
import net.librec.recommender.item.ItemEntry;
import net.librec.recommender.item.RecommendedItem;
import net.librec.recommender.item.RecommendedList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 *
 * @WIL-Lab
 * @Aldo-OG
 *
 */
public class OracleAlgo extends MatrixRecommender {
    private SequentialAccessSparseMatrix resultsMatrix;
    private TextDataConvertor resultsModel;
    private BiMap<Integer, String> userMappingInverse;
    private BiMap<Integer, String> itemMappingInverse;
    private BiMap<String, Integer> result_userMapping;
    private BiMap<String, Integer> result_itemMapping;

    @Override
    protected void trainModel() throws LibrecException {  // is this exception unnecessary?
        System.out.println("** Training model in OracleAlgo **");
        try {
            // Load resultsMatrix for this pass of OracleAlgo
            LoadResults();
            // Define Inverse mapping for this pass of OracleAlgo.
            // Flip requested inner ids to outer ids for split data.
            userMappingInverse = userMappingData.inverse();
            itemMappingInverse = itemMappingData.inverse();
            result_userMapping = resultsModel.getMatrix().getUserIds();
            result_itemMapping = resultsModel.getMatrix().getItemIds();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected double predict(int userIdx, int itemIdx) throws LibrecException { // is this exception unnecessary?
        // This is the Raw ID.
        String user_raw = userMappingInverse.get(userIdx);
        String item_raw = itemMappingInverse.get(itemIdx);
        double predictedRating = Double.NaN; // Assume NaN
        try {
            int user_inner = result_userMapping.get(user_raw); // Use Raw ID to get inner id's for results matrix.
            int item_inner = result_itemMapping.get(item_raw);  // **Null pointer exception always when trying to get item...
//            if(resultsMatrix.contains(user_inner,item_inner)) {  // If user and item ids are within...
                predictedRating = resultsMatrix.get(user_inner, item_inner);
                if (predictedRating == -2) {  // reached an empty user
                    predictedRating = 0.0;
                }
//            }
        }
        catch(NullPointerException e){} // intentionally pass when looking for value which doesn't exist
        catch(Exception e){ e.printStackTrace(); }
        return predictedRating;
    }

    /**
     * recommend
     * * predict the ranking scores in the test data
     *
     * @return predictive rating matrix
     * @throws LibrecException if error occurs during recommending
     */
//    @Override
//    public RecommendedList recommendRank() throws LibrecException {
//        LibrecDataList<AbstractBaseDataEntry> librecDataList = new BaseDataList<>();
//        for (int userIdx = 0; userIdx < numUsers; ++userIdx) {
//            BaseRankingDataEntry baseRankingDataEntry = new BaseRankingDataEntry(userIdx);
//            librecDataList.addDataEntry(baseRankingDataEntry);
//        }
//        return recommendRank(librecDataList);
//    }
//
//
//    /**
//     * ~ Auto-Method ~
//     *
//     * Loads result file and extract SparseTensor
//     *
//     * Assumes resultsMatrix exist data exists
//     *
//     * @throws ClassNotFoundException
//     * @throws IOException
//     * @throws LibrecException
//     */
    private void LoadResults() throws FileNotFoundException {  // this should return an arraylist of doubles, not String
        // Determine path
        Integer cvSplit = Integer.parseInt(conf.get("data.splitter.cv.index", "1"));
        String path = conf.get("dfs.result.dir") + "/out-" + cvSplit + ".txt";
        AutoDataAppender tempResultModel = new AutoDataAppender(path);
        try {
            tempResultModel.processData();
        } catch (IOException e) {
            e.printStackTrace();
        }
        SequentialAccessSparseMatrix temp = tempResultModel.getPreferenceMatrix();
        resultsMatrix = temp.clone();
        resultsModel = tempResultModel;
    }
}
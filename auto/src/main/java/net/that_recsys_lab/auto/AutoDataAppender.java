package net.that_recsys_lab.auto;

import com.google.common.collect.*;
import net.librec.data.convertor.TextDataConvertor;
import net.librec.math.structure.DataFrame;
import net.librec.math.structure.SequentialAccessSparseMatrix;
import net.librec.util.StringUtil;
import okio.BufferedSource;
import okio.Okio;
import okio.Source;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Made by @WangYuFeng and @liuxz
 *
 * Additions From:
 * @ WIL-Lab
 * @ ALdo-OG
 * @ Masoud Mansoury
 */

public class AutoDataAppender extends TextDataConvertor {
    /**
     * Log
     */
    private static final Log LOG = LogFactory.getLog(TextDataConvertor.class);
    /**
     * The size of the buffer
     */
    private static final int BSIZE = 1024 * 1024;

    /**
     * The default format of input data file
     */
    private static final String DATA_COLUMN_DEFAULT_FORMAT = "UIR";

    /**
     * The format of input data file
     */
    private String dataColumnFormat;

    /**
     * the path of the input data file
     */
    private String inputDataPath;

    /**
     * the threshold to binarize a rating. If a rating is greater than the threshold, the value will be 1;
     * otherwise 0. To disable this appender, i.e., keep the original rating value, set the threshold a negative value
     */
    private double binThold = -1.0;

    /**
     * user/item {raw id, inner id} map
     */
    private BiMap<String, Integer> userIds, itemIds;

    /**
     * time unit may depend on data sets, e.g. in MovieLens, it is unix seconds
     */
    private TimeUnit timeUnit = TimeUnit.SECONDS;

    /**
     * already loaded files/total files in dataDirectory
     */
    private float loadFilePathRate;

    /**
     * loaded data size /total data size in one data file
     */
    private float loadDataFileRate;

    /**
     * loaded data size /total data size in all data file
     */
    private float loadAllFileRate;


    private String[] m_header;
    private String[] m_attr;
    private String m_sep;
    private float m_fileRate;

    public AutoDataAppender(String path) {
        super(path);
        inputDataPath = path;
        this.m_sep = "[ \\t,]+";
    }

    /**
     * Process the input data.
     *
     * @throws IOException if the <code>inputDataPath</code> is not valid.
     */
    @Override
    public void processData() throws IOException {
        readDataAuto(inputDataPath);
    }


    private void readDataAuto(String... inputDataPath) throws IOException {
        LOG.info(String.format("Dataset: %s", Arrays.toString(inputDataPath)));
        matrix = new DataFrame();
        if (Objects.isNull(m_header)) {
            if (DATA_COLUMN_DEFAULT_FORMAT.toLowerCase().equals("uirt")) {
                m_header = new String[]{"user", "item", "rating", "datetime"};
                m_attr = new String[]{"STRING", "STRING", "NUMERIC", "DATE"};
            } else {
                m_header = new String[]{"user", "item", "rating"};
                m_attr = new String[]{"STRING", "STRING", "NUMERIC"};
            }
        }

        matrix.setAttrType(m_attr);
        matrix.setHeader(m_header);
        List<File> files = new ArrayList<>();
        SimpleFileVisitor<Path> finder = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                files.add(file.toFile());
                return super.visitFile(file, attrs);
            }
        };
        for (String path : inputDataPath) {
            Files.walkFileTree(Paths.get(path.trim()), finder);
        }
        int numFiles = files.size();
        int cur = 0;
        Pattern pattern = Pattern.compile(m_sep);
        for (File file : files) {
            try (Source fileSource = Okio.source(file);
                 BufferedSource bufferedSource = Okio.buffer(fileSource)) {
                String temp;
                while ((temp = bufferedSource.readUtf8Line()) != null) {
                    if ("".equals(temp.trim())) {
                        break;
                    }
                    String[] eachRow = pattern.split(temp);
                    for (int i = 0; i < m_header.length; i++) {
                        if (Objects.equals(m_attr[i], "STRING")) {
                            DataFrame.setId(eachRow[i], matrix.getHeader(i));
                        }
                    }
                    matrix.add(eachRow);
                }
                LOG.info(String.format("DataSet: %s is finished", StringUtil.last(file.toString(), 38)));
                cur++;
                m_fileRate = cur / numFiles;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
//        List<Double> ratingScale = matrix.getRatingScale();
//        if (ratingScale != null) {
//            LOG.info(String.format("rating Scale: %s", ratingScale.toString()));
//        }
        LOG.info(String.format("user number: %d,\t item number is: %d", matrix.numUsers(), matrix.numItems()));
    }

    /**
     * Return the number of users.
     *
     * @return number of users
     */
    public int numUsers() {
        return userIds.size();
    }
    /**
     * Return the number of items.
     *
     * @return number of items
     */
    public int numItems() {
        return itemIds.size();
    }

    /**
     * Return a user's inner id by his raw id.
     *
     * @param rawId raw user id as String
     * @return inner user id as int
     */
    public int getUserId(String rawId) {
        return userIds.get(rawId);
    }

    /**
     * Return an item's inner id by its raw id.
     *
     * @param rawId raw item id as String
     * @return inner item id as int
     */
    public int getItemId(String rawId) {
        return itemIds.get(rawId);
    }

    /**
     * Return user {rawid, inner id} mappings
     *
     * @return {@link #userIds}
     */
    public BiMap<String, Integer> getUserIds() {
        return userIds;
    }

    /**
     * Return item {rawid, inner id} mappings
     *
     * @return {@link #itemIds}
     */
    public BiMap<String, Integer> getItemIds() {
        return itemIds;
    }
}

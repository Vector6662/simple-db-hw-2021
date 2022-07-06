package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableId;
    private int ioCostPerPage;
    private TupleDesc td;
    private int numFields;
    private int tupleNums;
    private Map<Integer, IntHistogram> intHistogramMap;
    private Map<Integer, StringHistogram> stringHistogramMap;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.td = Database.getCatalog().getDatabaseFile(tableid).getTupleDesc();
        this.intHistogramMap = new HashMap<>();
        this.stringHistogramMap = new HashMap<>();
        this.numFields = td.numFields();

        Map<Integer, List> values = new HashMap<>(numFields); // <fieldNumber, values>，每一列的values记录
        for (int i = 0; i < numFields; i++) {
            if (td.getFieldType(i).equals(Type.INT_TYPE)){
                values.put(i, new ArrayList<Integer>());
            }else {
                values.put(i, new ArrayList<String>());
            }
        }
        SeqScan scan = new SeqScan(new TransactionId(), tableid);

        this.tupleNums = 0;
        try {
            scan.open();
            while (scan.hasNext()) {
                tupleNums++;
                Tuple t = scan.next();
                for (int i = 0; i < numFields; i++) {
                    Field field = t.getField(i);
                    if (field.getType().equals(Type.INT_TYPE)) {
                        values.get(i).add(field.hashCode());
                    } else {
                        values.get(i).add(field.toString());
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < numFields; i++) {
            if (td.getFieldType(i).equals(Type.INT_TYPE)) {
                List<Integer> list = values.get(i);
                IntHistogram h = new IntHistogram(NUM_HIST_BINS, Collections.min(list), Collections.max(list));
                intHistogramMap.put(i, h);
                for (Integer v : list) {
                    h.addValue(v);
                }
            } else {
                List<String> list = values.get(i);
                StringHistogram h = new StringHistogram(NUM_HIST_BINS);
                stringHistogramMap.put(i, h);
                for (String s : list) {
                    h.addValue(s);
                }
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return ioCostPerPage * ((HeapFile) Database.getCatalog().getDatabaseFile(tableId)).numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (selectivityFactor * tupleNums);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (constant.getType().equals(Type.INT_TYPE)) {
            return intHistogramMap.get(field).estimateSelectivity(op, constant.hashCode());
        }
        return stringHistogramMap.get(field).estimateSelectivity(op, constant.toString());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return tupleNums;
    }

}

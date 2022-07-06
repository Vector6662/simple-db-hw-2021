package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, List<Tuple>> groups;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groups = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field = tup.getField(gbfield);
        if (!groups.containsKey(field)) {
            groups.put(field, new ArrayList<>());
        }
        groups.get(field).add(tup);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new Iter();
    }

    class Iter implements OpIterator{
        private boolean isOpen=false;
        private Iterator<Field> iter;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (isOpen) return;
            isOpen = true;
            iter = groups.keySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen)
                return false;
            return iter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen)
                throw new NoSuchElementException("iter haven't been open");
            if (!iter.hasNext())
                throw new NoSuchElementException("no more elements");
            Field groupKey = iter.next();
            List<Tuple> tuples = groups.get(groupKey);
            Tuple res;
            if (gbfield != -1) { //(group,aggr)
                res = new Tuple(new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}));
                res.setField(0, groupKey);
                res.setField(1, new IntField(aggregate(tuples, afield)));
            } else { //(aggr)
                res = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
                res.setField(0, new IntField(aggregate(tuples, afield)));
            }
            return res;
        }

        int aggregate(List<Tuple> list, int index){
            IntStream intStream = list.stream().mapToInt(value -> value.getField(index).hashCode());
            switch (what) {
                case AVG:
                    return (int)intStream.average().getAsDouble();
                case MAX:
                    return intStream.max().getAsInt();
                case MIN:
                    return intStream.min().getAsInt();
                case SUM:
                    return intStream.sum();
                case COUNT:
                    return list.size();
            }
            return -1;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            if (gbfield != -1) {
                return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            } else {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            }
        }

        @Override
        public void close() {
            if (!isOpen) return;
            isOpen = false;
        }
    }

}

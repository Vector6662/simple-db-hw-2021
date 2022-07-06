package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return Arrays.stream(tdItems).iterator();
    }

    private static final long serialVersionUID = 1L;
    private TDItem[] tdItems;
    private int size; //size in bytes
    private Map<String, Integer> nameToIndex;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr == null || typeAr.length < 1)
            throw new RuntimeException("type[] is null, or at least one entry");
        int size = typeAr.length;
        TDItem[] items = new TDItem[size];
        for (int i = 0; i < typeAr.length; i++) {
            items[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
        init(items);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        if (typeAr == null || typeAr.length < 1)
            throw new RuntimeException("type[] is null, or at least one entry");
        int size = typeAr.length;
        TDItem[] items = new TDItem[size];
        for (int i = 0; i < typeAr.length; i++) {
            items[i] = new TDItem(typeAr[i], "Field_" + i);
        }
        init(items);
    }

    public TupleDesc(TDItem[] tdItems){
        init(tdItems);
    }

    void init(TDItem[] tdItems){
        if (tdItems == null){
            throw new NullPointerException();
        }
        this.tdItems = tdItems;
        nameToIndex = new HashMap<>(tdItems.length);
        for (int i = 0; i < tdItems.length; i++) {
            nameToIndex.put(tdItems[i].fieldName, i);
        }
        this.size = genSize(tdItems);
    }


    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems == null ? 0: tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= tdItems.length)
            throw new NoSuchElementException(String.format("fields length is %d, got i=%d", tdItems.length, i));
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= tdItems.length)
            throw new NoSuchElementException(String.format("fields length is %d, got i=%d", tdItems.length, i));
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        //todo 应该用一个map实现对应关系
        if (nameToIndex.containsKey(name)) {
            return nameToIndex.get(name);
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        if (tdItems == null) return 0;
        return size;
    }

    /**
     * calculate total size(bytes) of the tuple.
     */
    static int genSize(TDItem[] items){
        int size = 0;
        for (TDItem item : items) {
            // 原来tupleDesc的size是不包含fieldName的，这是因为这是计算每一个tuple占用的总字节数，而不是记录tupledesc的
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        if (td1 == null) return td2;
        if (td2 == null) return td1;
        int size1 = td1.numFields(), size2 = td2.numFields();
        TDItem[] dest = new TDItem[size1+size2];
        System.arraycopy(td1.tdItems, 0, dest, 0, size1);
        System.arraycopy(td2.tdItems, 0, dest, size1, size2);
        return new TupleDesc(dest);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (!(o instanceof TupleDesc))
            return  false;
        TupleDesc td = (TupleDesc) o;
        if (td.numFields() != this.numFields())
            return false;

        for (int i = 0; i < this.tdItems.length; i++) {
            if (this.tdItems[i].fieldType != td.getFieldType(i))
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        if (this.tdItems == null) return "";
        int size = this.numFields();
        String[] output = new String[size];
        for (int i = 0; i < this.tdItems.length; i++) {
            output[i] = tdItems[i].toString();
        }
        return Arrays.toString(output);
    }
}

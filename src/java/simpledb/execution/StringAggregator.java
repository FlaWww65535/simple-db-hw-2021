package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private Map<Field,Integer> aggreMap;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.aggreMap = new HashMap<Field,Integer>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gf;
        if(gbfield == NO_GROUPING){
            gf = null;
        }else{
            gf = tup.getField(gbfield);
        }
        StringField af = (StringField) tup.getField(afield);
        if(this.op == Op.COUNT){
            if(!aggreMap.containsKey(gf)) {
                aggreMap.put(gf, 1);
            }else{
                aggreMap.put(gf, aggreMap.get(gf)+1);
            }
        }else{
            throw new IllegalArgumentException("Aggregate not implemented.");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Type[]types;
        String[]names;
        TupleDesc td;
        List<Tuple>tuples = new ArrayList<>();

        if(gbfield == NO_GROUPING){
            types=new Type[]{Type.INT_TYPE};
            names = new String[]{"aggregateVal"};
            td = new TupleDesc(types,names);
            Tuple t = new Tuple(td);
            if(op == Op.COUNT){
                IntField intf =new IntField(aggreMap.get(null));
                t.setField(0,intf);
            }
            tuples.add(t);
        }else{
            types=new Type[]{gbfieldtype,Type.INT_TYPE};
            names = new String[]{"groupVal","aggregateVal"};
            td = new TupleDesc(types,names);
            if(op == Op.COUNT){
                for(Field f:aggreMap.keySet()){
                    Tuple t = new Tuple(td);
                    IntField intf= new IntField(aggreMap.get(f));
                    t.setField(0,f);
                    t.setField(1,intf);
                    tuples.add(t);
                }
            }
        }

        return new TupleIterator(td,tuples);
    }

}

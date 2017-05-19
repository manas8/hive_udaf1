package com.qubole.manas.myudf1;

import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.ql.exec.Description;

import org.apache.hadoop.hive.ql.exec.UDAF;

import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by manasb on 18/5/17.
 */

@Description(name="mean",value="_FUNC_(col,col)-computes mean",extended = "select col1, MeanFunc(value,array(values)) from table group by col1;")


public class MeanUDAF extends UDAF{

static final Log LOG = LogFactory.getLog(MeanUDAF.class.getName());

public static class MeanUDAFEvaluator implements UDAFEvaluator {

    public static class Column {

        HashMap<Integer,ArrayList<Integer>> keysmap = new HashMap<Integer,ArrayList<Integer>>();
    }

    private Column col = null;

    public MeanUDAFEvaluator() {
        super();
        init();
    }

    public void init() {
        LOG.debug("Initialize evaluator");
        col = new Column();
    }

    public boolean iterate(Integer seckey, ArrayList<Integer> vals) throws HiveException {
        LOG.debug("Iterating over each value for aggregation");
        if (col == null)
            throw new HiveException("item not initialized");
        ArrayList<Integer> vlist = col.keysmap.get(seckey);
        if(vlist==null)
        {
            ArrayList<Integer> nlist = new ArrayList<Integer>(vals);
            col.keysmap.put(seckey,nlist);
        }
        else
        {
            vlist.addAll(vals);
            col.keysmap.put(seckey, vlist);
        }
        return true;
    }

    public Column terminatePartial() {
        LOG.debug("return partially aggregated results");
        return col;
    }

    public boolean merge(Column other) {
        LOG.debug("merging by combining partial aggregation");
        if (other == null)
            return true;
        for(HashMap.Entry<Integer,ArrayList<Integer>> entry: other.keysmap.entrySet())
        {
            Integer skey = entry.getKey();
            ArrayList<Integer> vlist = col.keysmap.get(skey);
            if(vlist==null)
            {
                ArrayList<Integer> nlist = new ArrayList<Integer>(entry.getValue());
                col.keysmap.put(skey,nlist);
            }
            else
            {
                vlist.addAll(entry.getValue());
                col.keysmap.put(skey,vlist);
            }
        }
        return true;
    }

    public String terminate() {
        LOG.debug("at the end of last record of the group - returning null");
        String str="";
        for(HashMap.Entry<Integer,ArrayList<Integer>> entry : col.keysmap.entrySet())
        {
            str=str+"( "+entry.getKey()+"-"+entry.getValue()+") , ";
        }
        return str;

    }

}






}




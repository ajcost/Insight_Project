package edu.upenn.ajcost.SparkBatchProcess;

/*****************************
*
* @author adamcostarino
*
* Description : This interface extends a parametrized Java Map util
*               this allows for parametrized Map object to be written
*               to Cassandra.
*
*
******************************/

import java.util.Map;

public interface MapStringLong extends Map<String, Long> {
	
}

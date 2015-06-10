package fr.xebia.cascading.learn.level4;

import java.util.Arrays;
import java.util.Collections;

import cascading.flow.AssemblyPlanner.Context;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tap.hadoop.TemplateTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Up to now, operations were stacked one after the other. But the dataflow can
 * be non linear, with multiples sources, multiples sinks, forks and merges.
 */
public class NonLinearDataflow {
	
	/**
	 * Use {@link CoGroup} in order to know the party of each presidents.
	 * You will need to create (and bind) one Pipe per source.
	 * You might need to correct the schema in order to match the expected results.
	 * 
	 * presidentsSource field(s) : "year","president"
	 * partiesSource field(s) : "year","party"
	 * sink field(s) : "president","party"
	 * 
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch03s03.html
	 */
	public static FlowDef cogroup(Tap<?, ?, ?> presidentsSource, Tap<?, ?, ?> partiesSource,
			Tap<?, ?, ?> sink) {
		Pipe president = new Pipe("president");
		Pipe party = new Pipe("party");
		
		Fields declared = new Fields("year1", "president", "year2", "party");
		Fields groupOn = new Fields("year", String.class);
		Fields outputFields = new Fields("president", "party");
		Pipe join = new CoGroup(president, groupOn, party, groupOn, declared);
		join = new Retain(join, outputFields);
		
		return FlowDef
		    .flowDef()
		    .addSource(president, presidentsSource)
		    .addSource(party, partiesSource)
		    .addTail(join)
		    .addSink(join, sink);
	}
	
	/**
	 * Split the input in order use a different sink for each party. There is no
	 * specific operator for that, use the same Pipe instance as the parent.
	 * You will need to create (and bind) one named Pipe per sink.
	 * 
	 * source field(s) : "president","party"
	 * gaullistSink field(s) : "president","party"
	 * republicanSink field(s) : "president","party"
	 * socialistSink field(s) : "president","party"
	 * 
	 * In a different context, one could use {@link TemplateTap} in order to arrive to a similar results.
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch08s07.html
	 */
	public static FlowDef split(Tap<?, ?, ?> source,
			Tap<?, ?, ?> gaullistSink, Tap<?, ?, ?> republicanSink, Tap<?, ?, ?> socialistSink) {
	  Pipe data = new Pipe("data");
	  
	  // gaullist branch
	  String expression = "!party.equals(\"Gaullist\")";
	  ExpressionFilter expFilter = new ExpressionFilter(expression, String.class);
	  Pipe gaullist = new Pipe("gaullist", data);
	  gaullist = new Each(gaullist, expFilter);
	  
	  // republican branch
	  expression = "!party.equals(\"Republican\")";
	  expFilter = new ExpressionFilter(expression, String.class);
	  Pipe republican = new Pipe("republican", data);
	  republican = new Each(republican, expFilter);
	  
	  // socialist branch
	  expression = "!party.equals(\"Socialist\")";
	  expFilter = new ExpressionFilter(expression, String.class);
	  Pipe socialist = new Pipe("socialist", data);
	  socialist = new Each(socialist, expFilter);
	  
	  return FlowDef
	      .flowDef()
	      .addSource(data, source)
	      .addTails(Arrays.asList(gaullist, republican, socialist))
	      .addSink(gaullist, gaullistSink)
	      .addSink(republican, republicanSink)
	      .addSink(socialist, socialistSink);
	}

}

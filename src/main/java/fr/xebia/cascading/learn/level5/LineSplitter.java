package fr.xebia.cascading.learn.level5;


import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Tuple;

public class LineSplitter extends BaseOperation<Tuple> implements Function<Tuple> {
  
  public LineSplitter() {
    super(FreestyleJobs.WORD);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
    operationCall.setContext(Tuple.size(1));
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
    String toSplit = functionCall.getArguments().getString(0);
    toSplit = toSplit.replaceAll("(a,b,c)", "a b c").replaceAll("v2", "").replaceAll("[/']", " ").replaceAll("\\s-", "").replaceAll("[^a-zA-Z -]", "");
    for(String word : toSplit.toLowerCase().split("\\s+")) {
      functionCall.getContext().set(0, word);
      functionCall.getOutputCollector().add(functionCall.getContext());
    }
  }
}

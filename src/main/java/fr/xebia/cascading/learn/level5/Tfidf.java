package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Implement tf-idf algorithm, expects arguments in the following format/ order:
 * <p>
 * <br>
 *  - <b>Word Count (Long)</b>:
 *      The count of each word inside of the document that the word is
 *      found inside
 * <br>
 *  - <b>Max Word Count (Long)</b>:
 *      The maximum Word count of any word inside of the document given in
 *      the word is found inside
 * <br>
 *  - <b>Word Document Frequency (Long)</b>:
 *      The number of documents that this word can be found in
 * <br>
 *  - <b>Document Count (Long)</b>:
 *      The total number of unique Documents given in the input data
 *      set
 * </p>
 * 
 * <p>
 * <b>
 * Output:
 * </b>
 * <br>
 * A tuple with a single field (Double) containing the TF-IDF score of each word
 * </p>
 * 
 * @author  afcrowther
 */
public class Tfidf extends BaseOperation<Tuple> implements Function<Tuple> {

  public Tfidf(Fields outputField) {
    // expect 4 arguments or fail
    super(4, outputField);
  }
  
  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
    operationCall.setContext(Tuple.size(1));
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
    TupleEntry args = functionCall.getArguments();
    double wordCount = (double) args.getLong(0);
    double maxWordCount = (double) args.getLong(1);
    double wordDocFreq = (double) args.getLong(2);
    double docCount = (double) args.getLong(3);
    
    double tf = wordCount / maxWordCount;
    double idf = Math.log(docCount / wordDocFreq);
    double tfidf = tf * idf;
    
    functionCall.getContext().set(0, tfidf);
    functionCall.getOutputCollector().add(functionCall.getContext());
  }

}

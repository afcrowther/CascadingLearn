package fr.xebia.cascading.learn.level5;

import java.util.Collections;

import cascading.flow.FlowDef;
import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.MaxValue;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.assembly.Unique;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * You now know all the basics operators. Here you will have to compose them by yourself.
 */
public class FreestyleJobs {

  public static final Fields LINE = new Fields("line", String.class);
  public static final Fields WORD = new Fields("word", String.class);
  public static final Fields WORD_COUNT = new Fields("count", Long.class);
  public static final Fields MAX_WORD_COUNT = new Fields("max_word_count", Long.class);
  public static final Fields WORD_DOC_FREQ = new Fields("word_doc_freq", Long.class);
  public static final Fields DOC_FREQ_WORD = new Fields("doc_freq_word", String.class);
  public static final Fields ID = new Fields("id", String.class);
  public static final Fields CONTENT = new Fields("content", String.class);
  public static final Fields DOC_ID = new Fields("doc_id", String.class);
  public static final Fields DOC_ID_TEMP = new Fields("doc_id_temp", String.class);
  public static final Fields DOC_COUNT = new Fields("doc_count", Long.class);
  public static final Fields DOC_COUNT_WORD = new Fields("doc_count_word", String.class);
  public static final Fields TEMP_TALLY = new Fields("temp_tally", Long.class);
  public static final Fields JOIN_LEFT = new Fields("join_left", Long.class);
  public static final Fields JOIN_RIGHT = new Fields("join_right", Long.class);
  public static final Fields TFIDF = new Fields("tfidf", Double.class);
  
	/**
	 * Word count is the Hadoop "Hello world" so it should be the first step.
	 * 
	 * source field(s) : "line"
	 * sink field(s) : "word","count"
	 */
	public static FlowDef countWordOccurences(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		
		Pipe wordCount = new Each("wordCount", LINE, new LineSplitter(), WORD);
		wordCount = new CountBy(wordCount, WORD, WORD_COUNT);
				
		return FlowDef
		    .flowDef()
		    .addSource(wordCount, source)
		    .addTailSink(wordCount, sink);
	}
	
	/**
	 * Now, let's try a non trivial job : td-idf. Assume that each line is a
	 * document.
	 * 
	 * source field(s) : "id","content"
	 * sink field(s) : "docId","tfidf","word"
	 * 
	 * <pre>
	 * t being a term
	 * t' being any other term
	 * d being a document
	 * D being the set of documents
	 * Dt being the set of documents containing the term t
	 * 
	 * tf-idf(t,d,D) = tf(t,d) * idf(t, D)
	 * 
	 * where
	 * 
	 * tf(t,d) = f(t,d) / max (f(t',d))
	 * ie the frequency of the term divided by the highest term frequency for the same document
	 * 
	 * idf(t, D) = log( size(D) / size(Dt) )
	 * ie the logarithm of the number of documents divided by the number of documents containing the term t 
	 * </pre>
	 * 
	 * Wikipedia provides the full explanation
	 * @see http://en.wikipedia.org/wiki/tf-idf
	 * 
	 * If you are having issue applying functions, you might need to learn about field algebra
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch03s07.html
	 * 
	 * {@link First} or {@link Max} can be useful for isolating the maximum.
	 * 
	 * {@link HashJoin} can allow to do cross join.
	 * 
	 * PS : Do no think about efficiency, at least, not for a first try.
	 * PPS : You can remove results where tfidf < 0.1
	 */
	public static FlowDef computeTfIdf(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {

		Pipe tokenize = new Each("tokenize", CONTENT, new LineSplitter(), Fields.join(ID, WORD));

		// rename fields
		Identity identity = new Identity(Fields.join(DOC_ID, WORD));
		tokenize = new Each(tokenize, Fields.join(ID, WORD), identity);

		// branch to get word count per document
		Pipe wordCount = new Pipe("wordCount", tokenize);
		wordCount = new CountBy(wordCount, Fields.join(DOC_ID, WORD), WORD_COUNT);

		// branch out from word count to get max value of a word per doc
		Pipe maxValue = new Pipe("maxValue", wordCount);
		maxValue = new GroupBy(maxValue, DOC_ID);
		maxValue = new Every(maxValue, WORD_COUNT, new MaxValue(MAX_WORD_COUNT), Fields.ALL);
		maxValue = new Rename(maxValue, DOC_ID, DOC_ID_TEMP);

		// join back to word count so each word has a record of the max word count for a word inside its own document
		Pipe finalWordCount = new CoGroup("finalWordCount", wordCount, DOC_ID, maxValue, DOC_ID_TEMP);
		finalWordCount = new Discard(finalWordCount, DOC_ID_TEMP);

		// branch to get document count
		Pipe docCount = new Unique("docCount", tokenize, DOC_ID);
		docCount = new Each(docCount, new Insert(TEMP_TALLY, 1), Fields.ALL);
		docCount = new SumBy(docCount, TEMP_TALLY, TEMP_TALLY, DOC_COUNT, Long.class);
		docCount = new Each(docCount, new Insert(JOIN_RIGHT, 1), Fields.ALL);

		// branch to get word document frequency
		Pipe wordDocFreqCount = new Unique("wordDocFreq", tokenize, Fields.ALL);
		wordDocFreqCount = new Rename(wordDocFreqCount, WORD, DOC_FREQ_WORD);
		wordDocFreqCount = new CountBy(wordDocFreqCount, DOC_FREQ_WORD, WORD_DOC_FREQ);
		wordDocFreqCount = new Each(wordDocFreqCount, new Insert(JOIN_LEFT, 1), Fields.ALL);

		// combine the branches
		Pipe idf = new HashJoin(wordDocFreqCount, JOIN_LEFT, docCount, JOIN_RIGHT);
		Pipe tfidf = new CoGroup(finalWordCount, WORD, idf, DOC_FREQ_WORD);

		// do tf-idf algorithm
		tfidf = new Each(tfidf, Fields.join(WORD_COUNT, MAX_WORD_COUNT, WORD_DOC_FREQ, DOC_COUNT), new Tfidf(TFIDF), Fields.join(DOC_ID, TFIDF, WORD));
		
		// remove where tfidf < 0.1, and sort by reverse doc id and tfidf rating
		ExpressionFilter expFilter = new ExpressionFilter("tfidf < 0.1", Double.class);
		tfidf = new Each(tfidf, expFilter);
		DOC_ID.setComparator(DOC_ID, Collections.reverseOrder());
		Fields sortFields = Fields.join(TFIDF, WORD);
		sortFields.setComparators(
		    Collections.reverseOrder(),
		    Collections.reverseOrder());
		tfidf = new GroupBy(tfidf, DOC_ID, sortFields);
		
		return FlowDef
		    .flowDef()
		    .addSource(tokenize, source)
		    .addTailSink(tfidf, sink);
	}
	
}

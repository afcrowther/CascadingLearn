package fr.xebia.cascading.learn.level1;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * {@link Tuple} have a soft schema defined by {@link Fields}.
 */
public class BasicSchemaManipulation {

  /**
   * We want to replicate results of the plain copy (see level 0) but you are out of luck. The provided source has also
   * a spam "discardme" field. Use {@link Discard} in order to get rid of it. source field(s) : "discardme","line" sink
   * field(s) : "line" The result file has the names of the fields in the first line.
   * 
   * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch10s03.html
   */
  public static FlowDef discardField(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
    Pipe discard = new Pipe("discard");
    Fields toDiscard = new Fields("discardme");
    discard = new Discard(discard, toDiscard);

    return FlowDef
        .flowDef()
        .addSource(discard, source)
        .addTail(discard)
        .addSink(discard, sink);
  }

  /**
   * Same logic but we want to {@link Retain} only the "line" field. input field(s) : "donotretainme", "line" output
   * field(s) : "line"
   * 
   * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch10s05.html
   */
  public static FlowDef retainField(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
    Pipe retain = new Pipe("retain");
    Fields toRetain = new Fields("line");
    retain = new Retain(retain, toRetain);
    
    return FlowDef
        .flowDef()
        .addSource(retain, source)
        .addTail(retain)
        .addSink(retain, sink);
  }

  /**
   * Bad luck again, the field is badly named "renameme", {@link Rename} it into "line". source field(s) : "renameme"
   * sink field(s) : "line"
   * 
   * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch10s04.html
   */
  public static FlowDef renameField(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
    Pipe rename = new Pipe("rename");
    Fields toRename = new Fields("renameme");
    Fields renameTo = new Fields("line");
    rename = new Rename(rename, toRename, renameTo);
    
    return FlowDef
        .flowDef()
        .addSource(rename, source)
        .addTail(rename)
        .addSink(rename, sink);
  }

}

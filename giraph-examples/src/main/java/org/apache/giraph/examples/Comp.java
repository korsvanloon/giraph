package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import javax.validation.constraints.Null;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


// Stdout: /usr/local/hadoop/logs/userlogs/job_201506101104_0019/attempt_201506101104_0019_m_000001_0/stdout
// (the job and attempt directories changes)

// vertex output: $HADOOP_HOME/bin/hadoop dfs -cat /user/hduser/output/comp/p*

// remove outputdir: $HADOOP_HOME/bin/hadoop dfs -rmr /user/hduser/output/comp


// some more logging:  $HADOOP_HOME/bin/hadoop dfs -cat /user/hduser/output/comp/_logs/history/job_201506141010_0004_1434294486434_hduser_Giraph%3A+org.apache.giraph.examples.Comp
// (the job directory changes)

/**
 * Simple function to return the out degree for each vertex.
 */
@Algorithm(
        name = "comp Count"
)
public class Comp extends BasicComputation<
        IntWritable, Text, NullWritable, Comp.PairWritable> {

//  private static final Logger LOG =
//          Logger.getLogger(Comp.class);

  @Override
  public void compute(
          Vertex<IntWritable, Text, NullWritable> vertex,
          Iterable<PairWritable> messages) throws IOException {

    boolean hasMessages = false;

    // look if vertex has a triangle according to the messages
    for(PairWritable pair: messages) {
      hasMessages = true;
      for(Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {

        System.out.println("edge otherId: " + edge.getTargetVertexId().get());
        System.out.println(pair);

        if(edge.getTargetVertexId().get() == pair.getOtherId().get()) {
          saveTriangle(pair, vertex);
          break;
        }
      }
    }

    // split the neighbours into smaller and bigger vertices.
    List<IntWritable> smallerVertices = new ArrayList<>();
    List<IntWritable> greaterVertices = new ArrayList<>();

    for(Edge<IntWritable, NullWritable> edge : vertex.getEdges())
      if(edge.getTargetVertexId().get() < vertex.getId().get())
        smallerVertices.add(edge.getTargetVertexId());
      else
        greaterVertices.add(edge.getTargetVertexId());

    if(!hasMessages) {

      for (IntWritable smallerV : smallerVertices)
        for (IntWritable greaterV : greaterVertices) {

          // hey greaterV, do you have a connection to smallerV? kind regards V
          PairWritable tuple = new PairWritable(vertex.getId().get(), smallerV.get());
          sendMessage(greaterV, tuple);
        }
    }


    vertex.voteToHalt();
  }

  private void saveTriangle(PairWritable tuple, Vertex<IntWritable, Text, NullWritable> vertex) {
    String triangle = "[" + tuple.getOtherId().toString() + ", " + tuple.getFromId().toString() + ", " + vertex.getId().toString() + "]";
    System.out.println(triangle);
    vertex.setValue(new Text(vertex.getValue().toString() + triangle));
//    LOG.info(triangle);
  }


  public static class PairWritable implements Writable {

    private IntWritable fromId, otherId;

    public PairWritable() {
      set(new IntWritable(), new IntWritable());
    }

    public PairWritable(int a, int b) {
      set(new IntWritable(a), new IntWritable(b));
    }

    public PairWritable(IntWritable fromId, IntWritable otherId) {
      set(fromId, otherId);
    }

    public IntWritable getFromId() {
      return fromId;
    }

    public IntWritable getOtherId() {
      return otherId;
    }

    public void set(IntWritable fromId, IntWritable otherId) {
      this.fromId = fromId;
      this.otherId = otherId;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      fromId.write(dataOutput);
      otherId.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      fromId.readFields(dataInput);
      otherId.readFields(dataInput);
    }

    @Override
    public String toString() {
      return "(fromId: " + fromId.get() + ", otherId: " + otherId.get() + ")";
    }
  }
}
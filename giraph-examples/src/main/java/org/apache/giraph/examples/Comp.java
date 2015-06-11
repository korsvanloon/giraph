package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import javax.validation.constraints.Null;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple function to return the out degree for each vertex.
 */
@Algorithm(
        name = "comp Count"
)
public class Comp extends BasicComputation<
        IntWritable, NullWritable, NullWritable, TupleWritable> {

  private static final Logger LOG =
          Logger.getLogger(Comp.class);

  @Override
  public void compute(
          Vertex<IntWritable, NullWritable, NullWritable> vertex,
          Iterable<TupleWritable> messages) throws IOException {

    // look if vertex has a
    for(TupleWritable tuple :messages)
      for(Edge<IntWritable, NullWritable> edge : vertex.getEdges())
        if(edge.getTargetVertexId().get() == ((IntWritable) tuple.get(0)).get())
          saveTriangle(tuple, vertex);

    // split the neighbours into smaller and bigger vertices.
    List<IntWritable> smallerVertices = new ArrayList<>();
    List<IntWritable> greaterVertices = new ArrayList<>();

    for(Edge<IntWritable, NullWritable> edge : vertex.getEdges())
      if(edge.getTargetVertexId().get() < vertex.getId().get())
        smallerVertices.add(edge.getTargetVertexId());
      else
        greaterVertices.add(edge.getTargetVertexId());

    for(IntWritable smallerV : smallerVertices)
      for(IntWritable greaterV : greaterVertices)
        // hey greaterV, do you have a connection to smallerV? kind regards V
        sendMessage(greaterV, new TupleWritable(new Writable[]{smallerV, vertex.getId()}));

    vertex.voteToHalt();
  }

  private void saveTriangle(TupleWritable tuple, Vertex vertex) {
    String triangle = "[" + tuple.get(0).toString() + ", " + tuple.get(1).toString() + ", " + vertex.getId().toString() + "]";
    System.out.println(triangle);
    LOG.info(triangle);
  }

}
package org.apache.giraph.examples;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Simple function to return the out degree for each vertex.
 */
@Algorithm(
        name = "comp Count"
)
public class Comp extends BasicComputation<
        IntWritable, NullWritable, IntWritable, NullWritable> {

  @Override
  public void compute(
          Vertex<IntWritable, NullWritable, IntWritable> vertex,
          Iterable<NullWritable> messages) throws IOException {
//    LongWritable vertexValue = vertex.getValue();
//    vertexValue.set(vertex.getNumEdges());
//    vertex.setValue(vertexValue);

    vertex.voteToHalt();
  }
}
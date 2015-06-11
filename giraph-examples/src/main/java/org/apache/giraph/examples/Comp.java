package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;

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

  @Override
  public void compute(
          Vertex<IntWritable, NullWritable, NullWritable> vertex,
          Iterable<TupleWritable> messages) throws IOException {

    // look if vertex has a
    messages.forEach(tuple -> {
      vertex.getEdges().forEach(edge -> {
        if(edge.getTargetVertexId().get() == ((IntWritable) tuple.get(0)).get()) {
          saveTriangle(tuple, vertex);
        }
      });
    });

    // split the neighbours into smaller and bigger vertices.
    List<IntWritable> smallerVertices = new ArrayList<>();
    List<IntWritable> greaterVertices = new ArrayList<>();
    vertex.getEdges().forEach(edge -> {
      if(edge.getTargetVertexId().get() < vertex.getId().get()) {
        smallerVertices.add(edge.getTargetVertexId());
      } else {
        greaterVertices.add(edge.getTargetVertexId());
      }
    });

    smallerVertices.forEach(smallerV -> { // a
      greaterVertices.forEach(greaterV -> { // c
        // hey greaterV, do you have a connection to smallerV? kind regards V
        sendMessage(greaterV, new TupleWritable(new Writable[]{ smallerV, vertex.getId() }));
      });
    });

    vertex.voteToHalt();
  }

  private void saveTriangle(TupleWritable tuple, Vertex vertex) {
    System.out.println("[" + tuple.get(0).toString() + ", " + tuple.get(1).toString() + ", " + vertex.getId().toString() + "]");
  }
}
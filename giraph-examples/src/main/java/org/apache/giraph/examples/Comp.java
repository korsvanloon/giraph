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
      if(edgesContainTuple(vertex.getEdges(), tuple)) {
        saveTriangle(vertex, tuple);
      }
    });

    // split the neighbours into smaller and bigger vertices.
    List<IntWritable> smallerVertices = new ArrayList<>();
    List<IntWritable> greaterVertices = new ArrayList<>();
    vertex.getEdges().forEach(edge -> {
      if(vertex.getId().compareTo(edge.getTargetVertexId()) < 0) {
        smallerVertices.add(edge.getTargetVertexId());
      } else {
        greaterVertices.add(edge.getTargetVertexId());
      }
    });

    smallerVertices.forEach(smallerV -> { // a
      greaterVertices.forEach(greaterV -> { // c
        // send (a,b) to c
        sendMessage(greaterV, new TupleWritable(new Writable[]{ smallerV, vertex.getId() }));
      });
    });

    vertex.voteToHalt();
  }

  private boolean edgesContainTuple(Iterable<Edge<IntWritable, NullWritable>> edges, TupleWritable tuple) {
    boolean a = false, b = false;
    for(Edge edge : edges) {
      if(edge.getTargetVertexId().compareTo(tuple.get(0)) == 0) {
        a = true;
      }
      else if(edge.getTargetVertexId().compareTo(tuple.get(1)) == 0) {
        b = true;
      }
    }
    return a && b;
  }

  private void saveTriangle(Vertex vertex, TupleWritable tuple) {
    System.out.println("[" + tuple.get(0).toString() + ", " + vertex.getId().toString() + ", " + tuple.get(1).toString() + "]");
  }
}
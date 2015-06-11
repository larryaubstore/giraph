/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples;

import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.formats.GeneratedVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */
@Algorithm(
    name = "Link Map"
)
public class LinkMap extends BasicComputation<Text,
    Text, Text, Text> {
  /** Number of supersteps for this test */
  public static final int MAX_SUPERSTEPS = 30;
  /** Logger */
  private static final Logger LOG =
      Logger.getLogger(LinkMap.class);

  @Override
  public void compute(
      Vertex<Text, Text, Text> vertex,
      Iterable<Text> messages) throws IOException {

    System.out.println("LARRY ===================== MESSAGE");
    if (getSuperstep() >= 1) {
//      double sum = 0;
//      for (DoubleWritable message : messages) {
//        sum += message.get();
//      }
//      DoubleWritable vertexValue =
//          new DoubleWritable((0.15f / getTotalNumVertices()) + 0.85f * sum);
//      vertex.setValue(vertexValue);
//      aggregate(MAX_AGG, vertexValue);
//      aggregate(MIN_AGG, vertexValue);
//      aggregate(SUM_AGG, new LongWritable(1));
//      LOG.info(vertex.getId() + ": PageRank=" + vertexValue +
//          " max=" + getAggregatedValue(MAX_AGG) +
//          " min=" + getAggregatedValue(MIN_AGG));
    }

    if (getSuperstep() < MAX_SUPERSTEPS) {
//      long edges = vertex.getNumEdges();
//      sendMessageToAllEdges(vertex,
//          new DoubleWritable(vertex.getValue().get() / edges));
    } else {
      vertex.voteToHalt();
    }
  }

  /**
   * Worker context used with {@link SimplePageRankComputation}.
   */
  public static class LinkMapWorkerContext extends
      WorkerContext {

    @Override
    public void preApplication()
      throws InstantiationException, IllegalAccessException {
    }

    @Override
    public void postApplication() {

    }

    @Override
    public void preSuperstep() {
      if (getSuperstep() >= 3) {
//        LOG.info("aggregatedNumVertices=" +
//            getAggregatedValue(SUM_AGG) +
//            " NumVertices=" + getTotalNumVertices());
//        if (this.<LongWritable>getAggregatedValue(SUM_AGG).get() !=
//            getTotalNumVertices()) {
//          throw new RuntimeException("wrong value of SumAggreg: " +
//              getAggregatedValue(SUM_AGG) + ", should be: " +
//              getTotalNumVertices());
//        }
//        DoubleWritable maxPagerank = getAggregatedValue(MAX_AGG);
//        LOG.info("aggregatedMaxPageRank=" + maxPagerank.get());
//        DoubleWritable minPagerank = getAggregatedValue(MIN_AGG);
//        LOG.info("aggregatedMinPageRank=" + minPagerank.get());
      }
    }

    @Override
    public void postSuperstep() { }
  }


  /**
   * Simple VertexReader that supports {@link SimplePageRankComputation}
   */
  public static class LinkMapVertexReader extends
      GeneratedVertexReader<Text, Text, Text> {
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(LinkMapVertexReader.class);


    @Override
    public boolean nextVertex() {
      return totalRecords > recordsRead;
    }


    @Override
    public Vertex<Text, Text, Text>
    getCurrentVertex() throws IOException {
      Vertex<Text, Text, Text> vertex = getConf().createVertex();
//      LongWritable vertexId = new LongWritable(
//          (inputSplit.getSplitIndex() * totalRecords) + recordsRead);
//      DoubleWritable vertexValue = new DoubleWritable(vertexId.get() * 10d);
//      long targetVertexId =
//          (vertexId.get() + 1) %
//          (inputSplit.getNumSplits() * totalRecords);
//      float edgeValue = vertexId.get() * 100f;
//      List<Edge<LongWritable, FloatWritable>> edges = Lists.newLinkedList();
//      edges.add(EdgeFactory.create(new LongWritable(targetVertexId),
//          new FloatWritable(edgeValue)));
//      vertex.initialize(vertexId, vertexValue, edges);
//      ++recordsRead;
//      if (LOG.isInfoEnabled()) {
//        LOG.info("next: Return vertexId=" + vertex.getId().get() +
//            ", vertexValue=" + vertex.getValue() +
//            ", targetVertexId=" + targetVertexId + ", edgeValue=" + edgeValue);
//      }
      return vertex;
    }
  }

  public static class LinkMapVertexInputFormat extends
    GeneratedVertexInputFormat<Text, Text, Text> {
    @Override
    public VertexReader<Text, Text, Text> createVertexReader(InputSplit split,
      TaskAttemptContext context)
      throws IOException {
      return new LinkMapVertexReader();
    }
  }

  /**
   * Simple VertexOutputFormat that supports {@link SimplePageRankComputation}
   */
  public static class LinkMapVertexOutputFormat extends
      TextVertexOutputFormat<Text, Text, Text> {
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new LinkMapVertexWriter();
    }

    /**
     * Simple VertexWriter that supports {@link SimplePageRankComputation}
     */
    public class LinkMapVertexWriter extends TextVertexWriter {
      @Override
      public void writeVertex(
          Vertex<Text, Text, Text> vertex)
        throws IOException, InterruptedException {
        getRecordWriter().write(
            new Text(vertex.getId().toString()),
            new Text(vertex.getValue().toString()));
      }
    }
  }
}

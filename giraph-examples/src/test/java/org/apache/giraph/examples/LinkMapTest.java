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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.DefaultVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.MockUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.giraph.examples.SimpleShortestPathsComputation.SOURCE_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Contains a simple unit test for {@link SimpleShortestPathsComputation}
 */
public class LinkMapTest {

  /**
   * A local integration test on toy data
   */
  @Test
  public void testLinkMap() throws Exception {

    // a small four vertex graph
    String[] graph = new String[] {
        "[1,0,[[2,1],[3,3]]]",
        "[2,0,[[3,1],[4,10]]]",
        "[3,0,[[4,2]]]",
        "[4,0,[]]"
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    // start from vertex 1
    //SOURCE_ID.set(conf, 1);
    conf.setComputationClass(LinkMap.class);

    //conf.setComputationClass(SimpleShortestPathsComputation.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setVertexInputFormatClass(
        JsonLongDoubleFloatDoubleVertexInputFormat.class);
    conf.setVertexOutputFormatClass(
        JsonLongDoubleFloatDoubleVertexOutputFormat.class);

    // run internally
    Iterable<String> results = InternalVertexRunner.run(conf, graph);

    //Map<Long, Double> distances = parseDistancesJson(results);

//    conf.setOutEdgesClass(ArrayListEdges.class);
//    conf.setVertexInputFormatClass(IntIntNullTextVertexInputFormat.class);
//    conf.setVertexOutputFormatClass(
//        JsonLongDoubleFloatDoubleVertexOutputFormat.class);
//
//    // run internally
//    Iterable<String> results = InternalVertexRunner.run(conf, graph);

//    Map<Long, Double> distances = parseDistancesJson(results);
//
//    // verify results
//    assertNotNull(distances);
//    assertEquals(4, distances.size());
//    assertEquals(0.0, distances.get(1L), 0d);
//    assertEquals(1.0, distances.get(2L), 0d);
//    assertEquals(2.0, distances.get(3L), 0d);
//    assertEquals(4.0, distances.get(4L), 0d);
  }

  public static void main(String[] args) throws Exception {
      LinkMapTest linkMapTest = new LinkMapTest();
      linkMapTest.testLinkMap();
      System.out.println("Link Map!"); 
  }

}

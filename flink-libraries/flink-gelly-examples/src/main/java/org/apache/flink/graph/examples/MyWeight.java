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

package org.apache.flink.graph.examples;

        import org.apache.flink.api.common.functions.CrossFunction;
        import org.apache.flink.api.common.functions.FilterFunction;
        import org.apache.flink.graph.examples.data.EuclideanGraphData;
        import org.apache.flink.api.common.ProgramDescription;
        import org.apache.flink.api.common.functions.MapFunction;
        import org.apache.flink.api.java.DataSet;
        import org.apache.flink.api.java.ExecutionEnvironment;
        import org.apache.flink.api.java.tuple.Tuple2;
        import org.apache.flink.api.java.tuple.Tuple3;
        import org.apache.flink.graph.Edge;
        import org.apache.flink.graph.EdgeJoinFunction;
        import org.apache.flink.graph.Graph;
        import org.apache.flink.graph.Triplet;
        import org.apache.flink.graph.Vertex;
        import scala.Int;
        import org.apache.flink.graph.utils.Tuple2ToVertexMap;

        import java.io.Serializable;
        import java.util.LinkedList;
        import java.util.List;
        import java.lang.Math;

/**
 * From the list of Vertices with Euclidean coordinates returns List of weighted Edges
 * Main difference with existing class: does not need a list of Edges as an input
 * (sic!) Performs cross-product
 */


public class MyWeight implements ProgramDescription {

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Float, Float>> vertices = getVerticesDataSet(env);

        DataSet<Tuple3<Integer, Integer, Float>> edgesWithEuclideanWeight = vertices
                .cross(vertices)
                .with(new CrossFunction<Tuple3<Integer, Float, Float>, Tuple3<Integer, Float, Float>,
                        Tuple3<Integer, Integer, Float>>() {
                    @Override
                    public Tuple3<Integer, Integer, Float> cross(Tuple3<Integer, Float, Float> val1,
                                                                 Tuple3<Integer, Float, Float> val2) throws Exception {
                        // compute Euclidean distance of coordinates
                        Float dist = (float)Math.sqrt(Math.pow((double)val1.f1 - (double)val2.f1, 2) +
                                Math.pow((double)val1.f2 - (double)val2.f2, 2));
                        return new Tuple3<Integer, Integer, Float>(val1.f0, val2.f0, dist);
                    }
                })
                .filter(new FilterFunction<Tuple3<Integer, Integer, Float>>() {
                    @Override
                    public boolean filter(Tuple3<Integer, Integer, Float> value) throws Exception {
                        return (!value.f0.equals(value.f1));
                    }
                });

        // emit result
        if(fileOutput) {
            edgesWithEuclideanWeight.writeAsCsv(outputPath, "\n", "\t");
            //cyclic.getEdges().writeAsCsv(outputPath, "\n", ",");
            // result.getEdges().print();
            env.execute("Metric TSP solution");
        } else {
            System.out.println("TSPpath:");
            edgesWithEuclideanWeight.print();
            // cyclic.getEdges().print();
        }
    }

    @Override
    public String getDescription() {
        return "Find Euclidian Coordinates";
    }

    // ******************************************************************************************************************
    // UTIL METHODS
    // ******************************************************************************************************************

    private static boolean fileOutput = false;

    private static String verticesInputPath = null;

    private static String outputPath = null;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length == 2) {
                fileOutput = true;
                verticesInputPath = args[0];
                outputPath = args[1];
            } else {
                System.out.println("Executing Euclidean Graph Weighing example with default parameters and built-in default data.");
                System.out.println("Provide parameters to read input data from files.");
                System.out.println("See the documentation for the correct format of input files.");
                System.err.println("Usage: EuclideanGraphWeighing <input vertices path> " +
                        " <output path>");
                return false;
            }
        }
        return true;
    }

    private static DataSet<Tuple3<Integer, Float, Float>> getVerticesDataSet(ExecutionEnvironment env) {
        if (fileOutput) {
            return env.readCsvFile(verticesInputPath)
                    .fieldDelimiter(" ")
                    .lineDelimiter("\n")
                    .types(Integer.class, Float.class, Float.class);
        } else {
            Object[][] DEFAULT_Vert = new Object[][] {
                    new Object[]{1, 20833.3333f, 17100.0000f},
                    new Object[]{2, 20900.0000f, 17066.6667f},
                    new Object[]{3, 21300.0000f, 13016.6667f},
                    new Object[]{4, 21600.0000f, 14150.0000f},
                    new Object[]{5, 21600.0000f, 14966.6667f}
            };

            List<Tuple3<Integer, Float, Float>> vertList = new LinkedList<>();
            for (Object[] vertex : DEFAULT_Vert) {
                vertList.add(new Tuple3<Integer, Float, Float>((Integer) vertex[0], (Float) vertex[1], (Float) vertex[2]));
            }
            return env.fromCollection(vertList);
        }
    }

}

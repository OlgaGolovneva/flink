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

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeutils.base.DoubleComparator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.examples.data.MY_MSTDefaultData;
import org.apache.flink.graph.library.MY_MST;
import org.apache.flink.graph.utils.Tuple2ToVertexMap;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.lang.Math;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import java.io.FileWriter;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by Olga on 9/26/16.
 */

/**
 * Christofides' algorithm for the TSP
 * 2-approximation algorithm: it returns a cycle that is at most twice as long
 * as an optimal cycle: C ≤ 2 · OPT
*/

public class My_TSPExample implements ProgramDescription{

    @SuppressWarnings("serial")
    public static void main(String [] args) throws Exception {

        if(!parseParameters(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //get coordinates of vertices on plane
      //  DataSet<Tuple3<Long, Double, Double>> vertCoord = getEdgeDataSet(env);

        DataSet<Edge<Long, Double>> edges = getEdgeDataSet(env);
        //get Edges from coordinates
      /*  DataSet<Edge<Long, Double>> edges =
                vertCoord.cross(vertCoord)
                        .with(new EuclideanDistComputer())
                        .filter(new FilterFunction<Edge<Long, Double>>() {
                            @Override
                            public boolean filter(Edge<Long, Double> value) throws Exception {
                                return (value.getSource()!=value.getTarget());
                            }
                        })
                ;*/

        //System.out.println("CROSSPRODUCT IS DONE");

        Graph<Long, NullValue, Double> graph = Graph.fromDataSet(edges, env);

        // Find MST of the given graph
        Graph<Long, NullValue, Double> result=graph
                .run(new MY_MST<Long, NullValue, Double>(maxIterations));

        //System.out.println("MST IS READY");

        DataSet<Edge<Long, Double>> outres=result.getUndirected().getEdges().distinct();

        Graph<Long, NullValue, Double> cyclic=Graph.fromDataSet(outres,env);

        //Create a Hamiltonian cycle: Tuple2 < Vertex out, Vertex in >
        List<Tuple2<Long,Long>> tspList =
                HamCycle(cyclic.getEdges().collect(), numOfPoints);

        System.out.println("HamCycle IS READY: ");

        for (Tuple2<Long,Long> vert:tspList){
            System.out.println(vert);
        }


        //Collect edges - approximate TSP
        DataSet<Tuple2<Long,Long>> tspSet = env.fromCollection(tspList);

        DataSet<Tuple3<Long,Long,Double>> mytspPath = tspSet
                .join(graph.getEdges())
                    .where(0,1)
                    .equalTo(0,1)
                    .with(new JoinFunction<Tuple2<Long, Long>, Edge<Long, Double>, Tuple3<Long, Long, Double>>() {
                        @Override
                        public Tuple3<Long, Long, Double> join(Tuple2<Long, Long> first, Edge<Long, Double> second)
                                throws Exception {
                            return new Tuple3<Long, Long, Double>(first.f0,first.f1, second.getValue());
                        }
                    });

        // emit result
        if(fileOutput) {
            mytspPath.writeAsCsv(outputPath, "\n", ",");
            //cyclic.getEdges().writeAsCsv(outputPath, "\n", ",");
           // result.getEdges().print();
            env.execute("Metric TSP solution");
        } else {
            System.out.println("TSP cycle:");
            mytspPath.print();
           // cyclic.getEdges().print();
        }

    }

    @Override
    public String getDescription() {
        return "Minimum Spanning Tree Example";
    }

    // *************************************************************************
    // UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;

    private static String edgeInputPath = null;

    private static String outputPath = null;

    private static Integer maxIterations = MY_MSTDefaultData.MAX_ITERATIONS;

    private static Integer numOfPoints = 9;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if(args.length != 4) {
                System.err.println("Usage: <input edges path> <output path> <num of points> <num iterations>");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            outputPath = args[1];
            maxIterations = Integer.parseInt(args[3]);
            numOfPoints=Integer.parseInt(args[2]);
        } else {
            System.out.println("Executing Example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("Usage: <input edges path> <output path> <num of points> " +
                    " <num iterations>");
        }
        return true;
    }

/*    private static DataSet<Tuple3<Long, Double, Double>> getEdgeDataSet(ExecutionEnvironment env) {
        if (fileOutput) {
            return env.readCsvFile(edgeInputPath)
                    .fieldDelimiter(" ")
                    .lineDelimiter("\n")
                    .types(Long.class, Double.class, Double.class);
        } else {
            Object[][] DEFAULT_POINTS = new Object[][] {
                    new Object[]{1L, 1.0, 1.0},
                    new Object[]{2L, 3.0, 6.0},
                    new Object[]{3L, 5.0, 5.0},
                    new Object[]{4L, 5.0, 1.0},
                    new Object[]{5L, 6.0, 4.0},
                    new Object[]{7L, 8.0, 1.0},
                    new Object[]{8L, 7.0, 1.0},
                    new Object[]{9L, 3.0, 1.0}
            };
            List<Tuple3<Long, Double, Double>> edgeList = new LinkedList<Tuple3<Long, Double, Double>>();
            for (Object[] edge : DEFAULT_POINTS) {
                edgeList.add(new Tuple3<Long, Double, Double>((Long) edge[0], (Double) edge[1], (Double) edge[2]));
            }
            return env.fromCollection(edgeList);
        }
    }*/

    private static DataSet<Edge<Long, Double>> getEdgeDataSet(ExecutionEnvironment env) {
        if (fileOutput) {
            return env.readCsvFile(edgeInputPath)
                    .fieldDelimiter("\t")
                    .lineDelimiter("\n")
                    .types(Long.class, Long.class, Double.class)
                    .map(new Tuple3ToEdgeMap<Long, Double>());
        } else {
            return MY_MSTDefaultData.getDefaultEdgeDataSet(env);
        }
    }

    // CrossFunction computes the Euclidean distance between two Coord objects.
    private static class EuclideanDistComputer
            implements CrossFunction<Tuple3<Long, Double, Double>, Tuple3<Long, Double, Double>,
                Edge<Long, Double>> {

        @Override
        public Edge<Long, Double> cross(Tuple3<Long, Double, Double> c1, Tuple3<Long, Double, Double> c2) {
            // compute Euclidean distance of coordinates
            Double dist = Math.sqrt(Math.pow(c1.f1 - c2.f1, 2) + Math.pow(c1.f2 - c2.f2, 2));
            return new Edge<Long, Double>(c1.f0, c2.f0, dist);
        }
    }

    /*
     * Walk through all vertices exactly once using MST
     * Create list of pairs Source-Target
     * DFS-based method
     */
    private static List<Tuple2<Long,Long>> HamCycle (List<Edge<Long,Double>> edges, int n)
    {
        List<List<Long>> adj = new ArrayList<List<Long>>(n+1);
        for (int i=0; i<=n; i++)
            adj.add(new ArrayList<Long>());
        for(Edge<Long,Double> edge : edges)
            adj.get(edge.f0.intValue()).add(edge.f1);

        List<Tuple2<Long,Long>> result = new ArrayList<>();
        Long root = new Long(1);  //arbitrary vertex
        Long prev = root;
        Stack<Long> dfs = new Stack<Long>();
        dfs.push(root);
        boolean[] marked = new boolean[n+1];
        marked[root.intValue()]=true;
        while (!dfs.empty())
        {
            Long curVert = dfs.peek();
            dfs.pop();
            if (curVert != root) {
                result.add(new Tuple2<Long, Long>(prev, curVert));
            }

            for (Long neighbor: adj.get(curVert.intValue()))
            {
                if (!marked[neighbor.intValue()])
                {
                    dfs.push(neighbor);
                    marked[neighbor.intValue()]=true;
                }
            }
            prev = curVert;
        }
        result.add(new Tuple2<Long, Long>(prev, root));
        return result;
    }

}


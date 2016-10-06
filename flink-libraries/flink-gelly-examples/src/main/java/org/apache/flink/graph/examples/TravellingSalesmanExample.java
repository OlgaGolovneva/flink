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
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.examples.data.TSPDefaultData;
import org.apache.flink.graph.library.MinimumSpanningTree;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.types.NullValue;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 *
 * This example shows how to use Gelly's library method.
 * You can find all available library methods in {@link org.apache.flink.graph.library}.
 *
 * In particular, this example uses the {@link MinimumSpanningTree}
 * library method to compute an approximate solution to the Metric Travelling Salesman Problem (TSP).
 *
 * This class implements the 2-approximation algorithm for Metric version of TSP by Kou, Markowsky, and Berman
 * ["A fast algorithm for Steiner trees." Acta informatica 15.2 (1981): 141-145.]
 * It always returns a cycle that is at most twice as long
 * as an optimal cycle: C ≤ 2 · OPT.
 *
 * The input file is a plain text file and must be formatted as follows:
 * Edges are represented by tuples of srcVertexId, trgVertexId and EdgeValue (Distance between srcVertexId and trgVertexId) which are
 * separated by tabs. Edges themselves are separated by newlines. List of edges should correspond to undirected complete graph
 * For example: <code>1\t2\t0.3\n1\t3\t1.5\n</code> defines two edges,
 * 1-2 with weight 0.3, and 1-3 with weight 1.5.
 *
 * The algorithm guarantees 2-approximation only for Metric graphs,
 * i.e., graphs where edge weights satisfy triangle inequality: w(A,B)+w(B,C) >= w(A,C).
 * In particular, if edges represent distances between points in any Euclidean space (R^n), they do satisfy triangle inequality.
 *
 * Usage <code>MST &lt;edge path&gt; &lt;result path for TSP&gt; &lt;result path for MST&gt;
 * &lt;number of vertices&gt; &lt;number of iterations&gt; </code><br>
 *
 * If no parameters are provided, the program is run with default data from
 * {@link TSPDefaultData}.
 */

public class TravellingSalesmanExample implements ProgramDescription{

    @SuppressWarnings("serial")
    public static void main(String [] args) throws Exception {

        if(!parseParameters(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Edge<Integer, Float>> edges = getEdgeDataSet(env);

        // Find MST of the given graph
        Graph<Integer, NullValue, Float> result=Graph.fromDataSet(edges, env)
                .run(new MinimumSpanningTree(maxIterations));

        // Output MST
        if(fileOutput) {
            result.getEdges().writeAsCsv(outputPathMST, "\n", ",");
            env.execute("MST is written");
        }
        else {
            System.out.println("Minimum Spanning Tree");
            result.getEdges().print();
            System.out.println("Correct answer:\n" + TSPDefaultData.RESULTED_MST);
        }

        // List of MST edges
        List<Edge<Integer,Float>> MSTedges=result.getEdges().collect();

        // Compute Hamiltonian cycle as a list of edges (pairs of vertices)
        List<Tuple2<Integer,Integer>> tspList =
                HamCycle(MSTedges, numOfPoints);

        // Collect edges of Hamiltonian cycle in a DataSet
        DataSet<Tuple2<Integer,Integer>> tspSet = env.fromCollection(tspList);

        // Get edge weights from the original graph
        DataSet<Tuple3<Integer,Integer,Float>> tspCycle = tspSet
                .join(edges)
                .where(0,1)
                .equalTo(0,1)
                .with(new JoinFunction<Tuple2<Integer, Integer>, Edge<Integer, Float>, Tuple3<Integer, Integer, Float>>() {
                    @Override
                    public Tuple3<Integer, Integer, Float> join(Tuple2<Integer, Integer> first, Edge<Integer, Float> second)
                            throws Exception {
                        return new Tuple3<>(first.f0,first.f1, second.getValue());
                    }
                });

        // Output TSP
        if(fileOutput) {
            tspCycle.writeAsCsv(outputPath, "\n", ",");
            env.execute("Metric TSP solution");
        } else {
            System.out.println("TSP cycle:");
            tspCycle.print();
            System.out.println("Correct answer:\n" + TSPDefaultData.RESULTED_TSP);
        }

    }

    @Override
    public String getDescription() {
        return "Travelling Salesman Problem Example";
    }

    // *************************************************************************
    // UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;

    private static String edgeInputPath = null;

    private static String outputPath = null;

    private static String outputPathMST = null;

    private static Integer maxIterations = TSPDefaultData.MAX_ITERATIONS;

    private static Integer numOfPoints = 9;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if(args.length != 5) {
                System.err.println("Usage: <input edges path> <output path TSP> <output path MST> " +
                        "<num vertices> <num iterations>");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            outputPath = args[1];
            outputPathMST=args[2];
            maxIterations = Integer.parseInt(args[4]);
            numOfPoints=Integer.parseInt(args[3]);
        } else {
            System.out.println("Executing Example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("Usage: <input edges path> <output path TSP> <output path MST> " +
                    "<num vertices> <num iterations>");
        }
        return true;
    }

    private static DataSet<Edge<Integer, Float>> getEdgeDataSet(ExecutionEnvironment env) {
        if (fileOutput) {
            return env.readCsvFile(edgeInputPath)
                    .fieldDelimiter("\t")
                    .lineDelimiter("\n")
                    .types(Integer.class, Integer.class, Float.class)
                    .map(new Tuple3ToEdgeMap<Integer, Float>());
        } else {
            return TSPDefaultData.getDefaultEdgeDataSet(env);
        }
    }

    /**
     * This method computes a Hamiltonian cycle from a Minimum Spanning Tree.
     * For Metric graphs (w(A,B)+w(B,C) >= w(A,C)) the resulting Hamiltonian cycle is within a factor of two of the optimal one.
     * @param edges Edges of a Minimum Spanning Tree
     * @param n Number of vertices
     * @return list of pairs of vertices - edges of a Hamiltonian cycle computed from the provided Minimum Spanning Tree
     */
    private static List<Tuple2<Integer,Integer>> HamCycle (List<Edge<Integer,Float>> edges, int n)
    {
        // Compose adjacency lists of the graph.
        List<List<Integer>> adj = new ArrayList<>(n+1);
        for (int i=0; i<=n; i++)
            adj.add(new ArrayList<Integer>());
        for(Edge<Integer,Float> edge : edges)
            adj.get(edge.f0.intValue()).add(edge.f1);

        // List of edges of Hamiltonian Path
        List<Tuple2<Integer,Integer>> result = new ArrayList<>();

        // Arbitrary vertex is selected to be the root
        Integer root = 1;
        Integer prev = root;

        // marked[i]=true iff the vertex i has been visited.
        boolean[] marked = new boolean[n+1];
        marked[root.intValue()]=true;

        // Depth-First Search
        Stack<Integer> dfs = new Stack<>();
        dfs.push(root);

        while (!dfs.empty())
        {
            Integer curVert = dfs.peek();
            dfs.pop();
            if (!curVert.equals(root)) {
                result.add(new Tuple2<>(prev, curVert));
            }

            for (Integer neighbor: adj.get(curVert.intValue()))
            {
                if (!marked[neighbor.intValue()])
                {
                    dfs.push(neighbor);
                    marked[neighbor.intValue()]=true;
                }
            }
            prev = curVert;
        }
        result.add(new Tuple2<>(prev, root));
        return result;
    }

}

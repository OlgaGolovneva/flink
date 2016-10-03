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

package org.apache.flink.graph.library;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.*;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.GraphAlgorithm;

/**
 * This class implements Boruvka's algorithm ["On a certain minimal problem."
 * Praca Moravske Prirodovedecke Spolecnosti 3 (1926): 37-58.] for finding a Minimum Spanning Tree (MST).
 * A minimum spanning tree is a spanning tree of a connected, undirected graph. It connects
 * all the vertices together with the minimal total weighting for its edges.
 * A single graph can have many different spanning trees, this algorithm returns one of them.
 * If the input graph is disconnected, output is a Minimum Spanning Forest.
 *
 * This implementation does not take into account Edge Directions, i.e. the following edges in the
 * input graph are treated equivalently: Source -> Target, Source <- Target and Source <-> Target.
 * That is, every directed edge of the input graph is complemented with the reverse directed edge
 * of the same weight. The output graph is always undirected.
 *
 * The basic algorithm is described here: http://www.vldb.org/pvldb/vol7/p1047-han.pdf, and works
 * as follows: In the first phase, each vertex finds a minimum weight out-edge. These edges are
 * added to intermediate MST (i.e. MST at current iteration step). In the second phase, vertices
 * perform Summarization algorithm, using information about Connected Components in intermediate
 * MST. In the third phase, vertices perform edges cleaning. The graph gets smaller and smaller,
 * and the algorithm terminates when only unconnected vertices (i.e. no more edges) remain.
 *
 * The program returns the resulting graph, which represents an MST (or Forest) of the input graph.
 */

//public class MinimumSpanningTree <K, VV, EV extends Comparable<EV>>
public class MinimumSpanningTree
        implements GraphAlgorithm<Short, NullValue, Float, Graph<Short, NullValue, Float>> {

    //Maximum number of the while loop iterations
    private final Integer maxIterations;

    //Maximum number of iterations in GSAConnectedComponents
    //may differ from maxIterations
    private final Integer maxIterationsGSA;

    public MinimumSpanningTree(Integer maxIterations) {
        this.maxIterations = maxIterations;
        this.maxIterationsGSA = maxIterations;
    }

    @Override
    public Graph<Short, NullValue, Float> run(Graph<Short, NullValue, Float> graph) throws Exception {

        ExecutionEnvironment env = graph.getContext();

        DataSet<Edge<Short, Float>> undirectedGraphEdges = graph.getUndirected().getEdges().distinct();
        Graph<Short, NullValue, Float> undirectedGraph = Graph.fromDataSet(undirectedGraphEdges,env);

        // Create working graph with </String> Vertex Values - (!) Currently only String values are
        // supported in Summarization method.
        // Each Vertex Value corresponds to its Connected Component.
        // Each Edge Value stores its Original Source and Target vertices, and its original Edge Value (weight)
        // Vertex<VertexID,NullValue> -> Vertex<VertexID,ConComp=(String)VertexID>
        // Edge<SourceID,TargetID,Float> -> Edge<SourceID,TargetID,<Float,OriginalSourceID,OriginalTargetID>>

        Graph<Short, String, Tuple3<Float, Short, Short>> workGraph = undirectedGraph
                .mapVertices(new InitializeVert ())
                .mapEdges(new InitializeEdges ());


        // This graph will contain current solution, i.e., we will collect MST edges in mstGraph.
        Graph<Short, String, Float> mstGraph = null;


         // Iterate while adding new edges and Number of Iterations < maxIterations.
         //
         // "while" loop should be changed to Bulk/Delta iterations WHEN nested iterations will be supported in Flink.
         //
        int numberOfIterations=0;
        boolean addedNewEdges = true;
        while (addedNewEdges && numberOfIterations<maxIterations) {

            numberOfIterations++;

            // This set should later be defined as IterativeDataSet (WHEN nested iterations will be supported in Flink)
            DataSet<Edge<Short, Tuple3<Float, Short, Short>>> currentEdges = workGraph.getEdges();

            // Iterates function SelectMinWeight over all the vertices in graph.
            // Finds a shortest adjacent edge for each vertex.
            // Returns a (not necessary connected) subgraph consisting of those edges.
            DataSet<Edge<Short, Float>> minEnges =
                    workGraph.groupReduceOnEdges(new SelectMinWeight(), EdgeDirection.OUT);

            //Collect intermediate results
            if (mstGraph == null) {
                mstGraph = Graph.fromDataSet(workGraph.getVertices(), minEnges, env);
            } else {
                mstGraph = mstGraph.union(Graph.fromDataSet(workGraph.getVertices(), minEnges, env));
            }

             // Find connected components of mstGraph.
            DataSet<Vertex<Short, String>> updatedConnectedComponents =
                    mstGraph.run(new GSAConnectedComponents<Short, String, Float>(maxIterationsGSA));


             // Use Summarize to create/update SuperVertices in the ORIGINAL graph
             // "Clean" the resulting graph:
             // 1) delete loops
             // 2) select minWeightEdge and go back to original VV type

            Graph<Short, Summarization.VertexValue<String>, Summarization.EdgeValue<Tuple3<Float, Short, Short>>> compressedGraph =
                        Graph.fromDataSet(updatedConnectedComponents, currentEdges, env)
                            .run(new Summarization<Short, String, Tuple3<Float, Short, Short>>())
                            .filterOnEdges(new CleanEdges<Short, Summarization.EdgeValue<Tuple3<Float,Short,Short>>>());

            DataSet<Edge<Short, Tuple3<Float, Short, Short>>> finalEdges =
                    compressedGraph.getEdges()
                            .groupBy(0,1)
                            .reduceGroup(new SelectMinEdge());

            DataSet<Vertex<Short, String>> finalVertices = compressedGraph.mapVertices(new ExtractVertVal ()).getVertices();

            // Collect data for the next loop iteration or finish loop execution
            if (finalEdges.count()>0) {
                workGraph = Graph.fromDataSet(finalVertices, finalEdges, env);
            }
            else {
                // Quit loop.
                addedNewEdges = false;
            }
        }

        //Final solution
        DataSet<Edge<Short, Float>> mstEdges=Graph.fromDataSet(mstGraph.getEdges(),env).getUndirected().getEdges().distinct();

        return Graph.fromDataSet(graph.getVertices(), mstEdges, env);
    }

    // *************************************************************************
    // UTIL METHODS
    // *************************************************************************

    /**
     * Initialize vetrex value with a string representing its connected component number
     * (initially the connected component nmber equals the vertex ID).
     * The type </String> is used only to make Summarization work correctly,
     * should be later changed to the Vertex ID type (WHEN Summarization with non-String types will be supported in Flink).
     */

    private static final class InitializeVert implements MapFunction<Vertex<Short, NullValue>, String> {

        @Override
        public String map(Vertex<Short, NullValue> vertex) throws Exception {
            return Short.toString(vertex.f0);
        }
    }

    /**
     * Each Edge will store its original Source and Target Vertices along with its VV
     */

    private static final class InitializeEdges
            implements MapFunction<Edge<Short, Float>, Tuple3<Float, Short, Short>> {

        @Override
        public Tuple3<Float, Short, Short> map(Edge<Short, Float> edge) throws Exception {
            return new Tuple3(edge.f2,edge.f0,edge.f1);
        }
    }

    /**
     * For each vertex, find an edge with min(VV) and change VV type from </Tuple3> to </Float>.
     * If vertex has multiple edges with the same min(VV), output the edge with min(TargetSource).
     * This allows for graphs with not necessarily distinct edge weights.
     */

    private static final class SelectMinWeight
            implements EdgesFunction<Short,Tuple3<Float,Short,Short>,Edge<Short, Float>> {

        public void iterateEdges(Iterable<Tuple2<Short, Edge<Short, Tuple3<Float,Short,Short>>>> edges,
                                 Collector<Edge<Short, Float>> out) throws Exception
        {
            Float minVal = Float.MAX_VALUE;
            Edge<Short,Float> minEdge = null;
            for (Tuple2<Short, Edge<Short, Tuple3<Float,Short,Short>>> tuple : edges)
            {
                if (tuple.f1.getValue().f0.compareTo(minVal)<0)
                {
                    minVal = tuple.f1.getValue().f0;
                    // Original Source and Target.
                    minEdge=new Edge(tuple.f1.getValue().f1, tuple.f1.getValue().f2,minVal);
                }
                // We need to take introduce an order on edges of the same weight.
                else if (tuple.f1.getValue().f0.compareTo(minVal) == 0 && tuple.f1.getValue().f2.compareTo(minEdge.getTarget())<0){
                    minEdge=new Edge(tuple.f1.getValue().f1, tuple.f1.getValue().f2,minVal);
                }
            }
            if (minEdge!= null)
                out.collect(minEdge);
        }
    }

    /**
     * For given vertex, extract </String> VV out of </Summarization.VertexValue<String>>>
     */

    private static class ExtractVertVal implements MapFunction<Vertex<Short, Summarization.VertexValue<String>>, String> {

        @Override
        public String map(Vertex<Short, Summarization.VertexValue<String>> vertex) throws Exception {
            return vertex.f1.f0;
        }
    }

    /**
     * For given vertex, delete all self-loops.
     */

    @FunctionAnnotation.ForwardedFields("*->*")
    private static class CleanEdges<T extends Comparable<T>, ET> implements FilterFunction<Edge<T, ET>> {
        @Override
        public boolean filter(Edge<T, ET> value) throws Exception {
            return !(value.f0.compareTo(value.f1)==0);
        }
    }

    /**
     * For given source and target vertices, find all edges between them and leave only one edge with min(VV).
     * Also change VV type from </Summarization.EdgeValue</Tuple3>> to </Tuple3>.
     * If a source vertex has multiple edges with the same min(VV), output edge with min(OriginalTargetSource).
     * This allows for graphs with not necessarily distinct edge weights.
     */

    private static class SelectMinEdge implements GroupReduceFunction<Edge<Short, Summarization.EdgeValue<Tuple3<Float, Short, Short>>>,
            Edge<Short, Tuple3<Float,Short,Short>>> {

        @Override
        public void reduce(Iterable<Edge<Short, Summarization.EdgeValue<Tuple3<Float, Short, Short>>>> edges,
                           Collector<Edge<Short, Tuple3<Float,Short,Short>>> out) throws Exception {

            Float minVal = Float.MAX_VALUE;
            Edge<Short,Summarization.EdgeValue<Tuple3<Float,Short,Short>>> minEdge = null;
            Edge<Short,Tuple3<Float,Short,Short>> outEdge= new Edge();

            for (Edge<Short, Summarization.EdgeValue<Tuple3<Float,Short,Short>>> tuple : edges)
            {
                if (tuple.getValue().f0.f0.compareTo(minVal) < 0)
                {
                    minVal = tuple.getValue().f0.f0;
                    minEdge = tuple;
                }
                else if (tuple.getValue().f0.f0.compareTo(minVal) == 0 && tuple.getValue().f0.f2.compareTo(minEdge.getValue().f0.f2)<0){
                    minEdge=tuple;
                }
            }
            if (minEdge!= null) {
                outEdge.setSource(minEdge.getSource());
                outEdge.setTarget(minEdge.getTarget());
                outEdge.setValue(minEdge.getValue().f0);
                out.collect(outEdge);
            }
        }
    }
}
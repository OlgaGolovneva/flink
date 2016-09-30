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

/**
 * Created by Olga on 9/12/16.
 */

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
 * This implementation uses Boruvka's algorithm to find a Minimum Spanning Tree (MST)
 * A minimum spanning tree is a spanning tree of a connected, undirected graph. It connects
 * all the vertices together with the minimal total weighting for its edges.
 * A single graph can have many different spanning trees, this algorithm returns one of them
 * Implementation ALLOWS FOR disconnected, directed input graph (See MY_MSTDefaultData for Examples)
 * If the input graph is disconnected, output is a Minimum Spanning Forest
 * Implementation does not take into account Edge Directions, i.e. the following edges in the
 * input graph are treated equivalently: Source -> Target, Source <- Target and Source <-> Target.
 * That is, every directed edge of the input graph is complemented with the reverse directed edge
 * of the same weight (the complementary edges never appear in the output).

 * The basic algorithm is descibed here: http://www.vldb.org/pvldb/vol7/p1047-han.pdf, and works
 * as follows: In the first phase, each vertex finds a minimum weight out-edge. These edges are
 * added to intermediate MST (i.e. MST at current iteration step). In the second phase, vertices
 * perform Summarization algorithm, using information about Connected Components in intermediate
 * MST. In the third phase, vertices perform edges cleaning. The graph gets smaller and smaller,
 * and the algorithm terminates when only unconnected vertices (i.e. no more Edges) remain.
 * The program returns the resulting graph, which represents the MST (or Forest) of the input graph
 */

public class MY_MST <K, VV, EV extends Comparable<EV>>
        implements GraphAlgorithm<Long, NullValue, Double, Graph<Long, NullValue, Double>> {

    //Maximum number of the while loop iterations
    private Integer maxIterations;
    //Maximum number of iterations in GSAConnectedComponents
    //private Integer maxIterations2=10;
    private Integer maxIterations2;

    public MY_MST(Integer maxIterations) {
        this.maxIterations = maxIterations;
        this.maxIterations2 = maxIterations;
    }

    final TypeInformation<Long> longType = BasicTypeInfo.LONG_TYPE_INFO;
    final TypeInformation<Double> doubleType = BasicTypeInfo.DOUBLE_TYPE_INFO;

    @Override
    public Graph<Long, NullValue, Double> run(Graph<Long, NullValue, Double> graph) throws Exception {

        ExecutionEnvironment env =graph.getContext();

        DataSet<Edge<Long, Double>> undirectedGraphE = graph.getUndirected().getEdges().distinct();

        Graph<Long, NullValue, Double> undirectedGraph = Graph.fromDataSet(undirectedGraphE,env);

        /**
         * Create working graph with </String> Vertex Values - (!) Currently only String values are
         * supported in Summarization method.
         *
         * Each Vertex Value corresponds to its Connected Component
         * Each Edge Value stores its Original Source and Target values, and Edge Value
         * Vertex<VertexID,NullValue> -> Vertex<VertexID,ConComp=(String)VertexID>
         * Edge<SourceID,TargetID,Double> -> Edge<SourceID,TargetID,<Double,OriginalSourseID,OriginalTargetID>>
         */
        Graph<Long, String, Double> InVertGr=undirectedGraph.mapVertices(new InitializeVert ());

        Graph<Long, String, Tuple3<Double, Long, Long>> graphWork =
                InVertGr.mapEdges(new InitializeEdges ());

        /**
         * Create MSTGraph with NO Edges
         * This graph will contain intermediate solution
         */

        Graph<Long, String, Double> MSTGraph = null;

        /**
         * Iterate while working graph has more than 1 Vertex and Number of Iterations < maxIterations
         *
         * "while" loop has to be changed to Bulk/Delta iterations WHEN nested iterations will be supported in Flink
         */
        int numberOfIterations=0;
        while (graphWork.getVertices().count()>1 && numberOfIterations<maxIterations) {

            numberOfIterations++;

            System.out.println("ITERATION NUMBER: "+numberOfIterations);
            //This set may later be defined as IterativeDataSet
            DataSet<Edge<Long, Tuple3<Double, Long, Long>>> CurrentEdges = graphWork.getEdges();

            /**
             * Find a (not necessary connected) subgraph, which contains for each vertex Edges with min(EV)
             * Iterates function SelectMinWeight over all the vertices in graph
             */
            DataSet<Edge<Long, Double>> MinEdgeTuples =
                    graphWork.groupReduceOnEdges(new SelectMinWeight (), EdgeDirection.OUT);

            //Collect intermediate results
            if (MSTGraph == null) {
                MSTGraph = Graph.fromDataSet(graphWork.getVertices(), MinEdgeTuples, env);
            } else {
                MSTGraph = MSTGraph.union(Graph.fromDataSet(graphWork.getVertices(), MinEdgeTuples, env));
            }

            /**
             * Use GSAConnectedComponents to find connected components in the output graph
             */

            DataSet<Vertex<Long, String>> UpdateConComp =
                    MSTGraph.run(new GSAConnectedComponents<Long, String, Double>(maxIterations2));

            /**
             * Use Summarize to create/edit SuperVertices in ORIGINAL graph
             */

            Graph<Long, Summarization.VertexValue<String>, Summarization.EdgeValue<Tuple3<Double, Long, Long>>> CompressedGraph1 =
                    Graph.fromDataSet(UpdateConComp, CurrentEdges, env)
                            .run(new Summarization<Long, String, Tuple3<Double, Long, Long>>());

            /**
             * Now we want to "clean" our graph: 1) delete loops
             * 2) select minWeightEdge and go back to original VV type
             */

            Graph<Long, Summarization.VertexValue<String>, Summarization.EdgeValue<Tuple3<Double, Long, Long>>> CompressedGraph =
                    CompressedGraph1.filterOnEdges(new CleanEdges<Long, Summarization.EdgeValue<Tuple3<Double,Long,Long>>>());

            DataSet<Edge<Long, Tuple3<Double, Long, Long>>> FinalEdges =
                    CompressedGraph.getEdges()
                    .groupBy(0,1)
                    .reduceGroup(new SelectMinEdge());


            DataSet<Vertex<Long, String>> FinalVertices = CompressedGraph.mapVertices(new ExtractVertVal ()).getVertices();

            //collect data for the next loop iteration or finish loop execution
            if (FinalEdges.first(1).count()>0) {
                graphWork = Graph.fromDataSet(FinalVertices, FinalEdges, env);
            }
            else {
                numberOfIterations=maxIterations;
            }
        }


        System.out.println("ITERATIONS ARE FINISHED");

        //Final solution
        DataSet<Edge<Long, Double>> MST=Graph.fromDataSet(MSTGraph.getEdges().distinct(),env).getUndirected()
                .getEdges().distinct();

        Graph<Long, NullValue, Double> MSTout=Graph.fromDataSet(graph.getVertices(), MST, env).intersect(graph,true);

        return MSTout;
    }

    // *************************************************************************
    // UTIL METHODS
    // *************************************************************************

    /**
     * Each VV corresponds to its </String> Connected Component (CC) in MST Graph.
     * Before iterations, the number of CC is equal to the number of Vertices (MST Graph has NO edges)
     * </String> is used only to make Summarization work correctly
     */
    @SuppressWarnings("serial")
    public static final class InitializeVert implements MapFunction<Vertex<Long, NullValue>, String> {

        public InitializeVert() {}
        @Override
        public String map(Vertex<Long, NullValue> vertex) throws Exception {
            return Long.toString(vertex.f0);
        }
    }

    /**
     * Each Edge will store its original Source and Target Vertices along with its VV
     */
    public static final class InitializeEdges
            implements MapFunction<Edge<Long, Double>, Tuple3<Double, Long, Long>> {

        public InitializeEdges() {}
        @Override
        public Tuple3<Double, Long, Long> map(Edge<Long, Double> edge) throws Exception {
            return new Tuple3(edge.f2,edge.f0,edge.f1);
        }
    }

    /**
     * For given vertex find edge with min(VV) and change VV type from </Tuple3> to </Double>.
     * If vertex has multiple edges with the same min(VV), output edge with min(TargetSource)
     * This allows for graphs with not necessarily distinct edge weights
     */

    private static final class SelectMinWeight
            implements EdgesFunction<Long,Tuple3<Double,Long,Long>,Edge<Long, Double>> {

        public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Tuple3<Double,Long,Long>>>> edges,
                                 Collector<Edge<Long, Double>> out) throws Exception
        {
            Double minVal = Double.MAX_VALUE;
            Edge<Long,Double> minEdge = null;
            for (Tuple2<Long, Edge<Long, Tuple3<Double,Long,Long>>> tuple : edges)
            {
                if (tuple.f1.getValue().f0 < minVal)
                {
                    minVal = tuple.f1.getValue().f0;
                    //Original Source and Target!!!!
                    minEdge=new Edge(tuple.f1.getValue().f1, tuple.f1.getValue().f2,minVal);
                }
                //we need to take into account equal edges!
                else if (tuple.f1.getValue().f0 == minVal && tuple.f1.getValue().f2<minEdge.getTarget()){
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

    @SuppressWarnings("serial")
    private static class ExtractVertVal implements MapFunction<Vertex<Long, Summarization.VertexValue<String>>, String> {

        @Override
        public String map(Vertex<Long, Summarization.VertexValue<String>> vertex) throws Exception {
            return vertex.f1.f0;
        }
    }

    /**
     * For given vertex, delete all self Edges
     */

    @SuppressWarnings("serial")
    @FunctionAnnotation.ForwardedFields("*->*")
    private static class CleanEdges<T extends Comparable<T>, ET> implements FilterFunction<Edge<T, ET>> {
        @Override
        public boolean filter(Edge<T, ET> value) throws Exception {
            return !(value.f0.compareTo(value.f1)==0);
        }
    }

    /**
     * For given vertex find all duplicated edges. Select edge with min(VV) and change VV type from </Summarization.EdgeValue</Tuple3>>
     * to </Tuple3>.
     * If vertex has multiple edges with the same min(VV), output edge with min(OriginalTargetSource)
     * This allows for graphs with not necessarily distinct edge weights
     */

    public static class SelectMinEdge implements GroupReduceFunction<Edge<Long, Summarization.EdgeValue<Tuple3<Double, Long, Long>>>,
            Edge<Long, Tuple3<Double,Long,Long>>> {

        @Override
        public void reduce(Iterable<Edge<Long, Summarization.EdgeValue<Tuple3<Double, Long, Long>>>> edges,
                Collector<Edge<Long, Tuple3<Double,Long,Long>>> out) throws Exception {

            Double minVal = Double.MAX_VALUE;
            Edge<Long,Summarization.EdgeValue<Tuple3<Double,Long,Long>>> minEdge = null;
            Edge<Long,Tuple3<Double,Long,Long>> outEdge= new Edge();

            for (Edge<Long, Summarization.EdgeValue<Tuple3<Double,Long,Long>>> tuple : edges)
            {
                if (tuple.getValue().f0.f0 < minVal)
                {
                    minVal = tuple.getValue().f0.f0;
                    minEdge = tuple;
                }
                else if (tuple.getValue().f0.f0 == minVal && tuple.getValue().f0.f2<minEdge.getValue().f0.f2){
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
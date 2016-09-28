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

package org.apache.flink.graph.examples.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Olga on 9/12/16.
 */
public class MY_MSTDefaultData {

    public static final Integer MAX_ITERATIONS = 3;

    //CHOOSE ANY TEST OR CREATE A NEW ONE

    /** TEST 1
    * Simple Data Set
    * in this case, there is ONLY 1 MST
    */
    public static final String EDGES = "1\t2\t1.0\n" + "1\t4\t3.0\n" + "2\t3\t6.0\n"+ "2\t4\t5.0\n"+ "2\t5\t1.0\n" +
            "3\t5\t5.0\n" + "3\t6\t2.0\n" + "4\t5\t1.0\n" + "5\t6\t4.0";

    public static final Object[][] DEFAULT_EDGES = new Object[][] {
            new Object[]{1L, 2L, 1.0},
            new Object[]{1L, 4L, 3.0},
            new Object[]{2L, 3L, 6.0},
            new Object[]{2L, 4L, 5.0},
            new Object[]{2L, 5L, 1.0},
            new Object[]{3L, 5L, 5.0},
            new Object[]{3L, 6L, 2.0},
            new Object[]{4L, 5L, 1.0},
            new Object[]{5L, 6L, 4.0}
    };

    public static final String RESULTED_MST =  "1\t2\t1.0\n" + "2\t5\t1.0\n" + "3\t6\t2.0\n" +
            "4\t5\t1.0\n" + "5\t6\t4.0";

    /** TEST 2
     * Cyclic undirected graph with equal EV. Multiple outputs are possible
     */

/*    public static final String EDGES = "1\t2\t1.0\n" + "1\t3\t1.0\n" + "2\t1\t1.0\n"+ "2\t3\t1.0\n"+ "3\t1\t1.0\n" +
            "3\t2\t1.0";

    public static final Object[][] DEFAULT_EDGES = new Object[][] {
            new Object[]{1L, 2L, 1.0},
            new Object[]{1L, 3L, 1.0},
            new Object[]{2L, 1L, 1.0},
            new Object[]{2L, 3L, 1.0},
            new Object[]{3L, 1L, 1.0},
            new Object[]{3L, 2L, 1.0}
    };

    public static final String RESULTED_MST =  "1\t2\t1.0\n" +  "1\t3\t1.0\n" + "2\t1\t1.0\n" + "3\t1\t1.0";
*/

    /** TEST 3
     * Disconnected directed graph with cycles
     */

/*    public static final String EDGES = "1\t2\t1.0\n" + "1\t4\t3.0\n" + "2\t3\t6.0\n"+ "2\t4\t5.0\n"+ "2\t5\t1.0\n" +
            "3\t5\t5.0\n" + "3\t6\t2.0\n" + "4\t5\t1.0\n" + "5\t6\t4.0\n"+"7\t8\t1.0\n" + "7\t9\t1.0\n" + "8\t7\t1.0\n"
            + "8\t9\t1.0\n"+ "9\t7\t1.0\n" + "9\t8\t1.0";

    public static final Object[][] DEFAULT_EDGES = new Object[][] {
            new Object[]{1L, 2L, 1.0},
            new Object[]{1L, 4L, 3.0},
            new Object[]{2L, 3L, 6.0},
            new Object[]{2L, 4L, 5.0},
            new Object[]{2L, 5L, 1.0},
            new Object[]{3L, 5L, 5.0},
            new Object[]{3L, 6L, 2.0},
            new Object[]{4L, 5L, 1.0},
            new Object[]{5L, 6L, 4.0},
            new Object[]{7L, 8L, 1.0},
            new Object[]{7L, 9L, 1.0},
            new Object[]{8L, 7L, 1.0},
            new Object[]{8L, 9L, 1.0},
            new Object[]{9L, 7L, 1.0},
            new Object[]{9L, 8L, 1.0}
    };

    public static final String RESULTED_MST =  "1\t2\t1.0\n" + "2\t5\t1.0\n" + "3\t6\t2.0\n" +
            "4\t5\t1.0\n" + "5\t6\t4.0\n"+"7\t8\t1.0\n" +  "7\t9\t1.0\n" + "8\t7\t1.0\n" + "9\t7\t1.0";
*/

    /** TEST 4
     * Cyclic directed graph with distinct EV
     */

/*    public static final String EDGES = "1\t2\t1.0\n" + "1\t3\t4.0\n" + "2\t1\t2.0\n"+ "2\t3\t8.0\n"+ "3\t1\t3.0\n" +
            "3\t2\t6.0";

    public static final Object[][] DEFAULT_EDGES = new Object[][] {
            new Object[]{1L, 2L, 1.0},
            new Object[]{1L, 3L, 4.0},
            new Object[]{2L, 1L, 2.0},
            new Object[]{2L, 3L, 8.0},
            new Object[]{3L, 1L, 3.0},
            new Object[]{3L, 2L, 6.0}
    };

    public static final String RESULTED_MST =  "1\t2\t1.0\n" + "3\t1\t3.0";
*/
    // END OF TEST SETS

    public static DataSet<Edge<Long, Double>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

        List<Edge<Long, Double>> edgeList = new LinkedList<Edge<Long, Double>>();
        for (Object[] edge : DEFAULT_EDGES) {
            edgeList.add(new Edge<Long, Double>((Long) edge[0], (Long) edge[1], (Double) edge[2]));
        }
        return env.fromCollection(edgeList);
    }

    private MY_MSTDefaultData() {}

}

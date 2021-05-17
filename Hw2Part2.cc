/*
 * MIT License
 *
 * Copyright (c) 2021 Chenming C
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <iostream>

#include <stdio.h>
#include <string.h>
#include <math.h>

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) PageRankVertex##name

#define EPS 1e-6
#define MAX 1e6

int start_id = 0;

class VERTEX_CLASS_NAME(InputFormatter) : public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex = n;
        return m_total_vertex;
    }

    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge = n;
        return m_total_edge;
    }

    int getVertexValueSize() {
        m_n_value_size = sizeof(double);
        return m_n_value_size;
    }

    int getEdgeValueSize() {
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }

    int getMessageValueSize() {
        m_m_value_size = sizeof(double);
        return m_m_value_size;
    }


    void loadGraph() {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        double weight = 0;
        double max = MAX;
        int out_degree = 0;
        const char *line = getEdgeLine();

        sscanf(line, "%lld %lld %lf", &from, &to, &weight);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++out_degree;
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line = getEdgeLine();

            double temp_weight;
            sscanf(line, "%lld %lld %lf", &from, &to, &temp_weight);
            if (last_vertex != from) {
                addVertex(last_vertex, &max, out_degree);
                last_vertex = from;
                out_degree = 1;
            } else {
                ++out_degree;
            }
            addEdge(from, to, &temp_weight);
        }
        addVertex(last_vertex, &max, out_degree);
    }
};

class VERTEX_CLASS_NAME(OutputFormatter) : public OutputFormatter {
public:
    void writeResult() {
        int64_t vid;
        double value;
        char s[1024];

        for (ResultIterator r_iter; !r_iter.done(); r_iter.next()) {
            r_iter.getIdValue(vid, &value);
            int n = sprintf(s, "%lld: %lf\n", (unsigned long long) vid, value);
            writeNextResLine(s, n);
        }
    }
};

// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator) : public Aggregator<double> {
public:
    void init() {
        m_global = 0;
        m_local = 0;
    }

    void *getGlobal() {
        return &m_global;
    }

    void setGlobal(const void *p) {
        m_global = *(double *) p;
    }

    void *getLocal() {
        return &m_local;
    }

    void merge(const void *p) {
        m_global += *(double *) p;
    }

    void accumulate(const void *p) {
        m_local += *(double *) p;
    }
};

class VERTEX_CLASS_NAME() : public Vertex<double, double, double> {
public:
    void compute(MessageIterator *pmsgs) {
        // check last step has node been changed or not
        if (getSuperstep() > 1) {
            double global_val = *(double *) getAggrGlobal(0);
            if (global_val < EPS) {
                voteToHalt();
                return;
            }
        }

        // receive msg from last step and update self
        double current = getValue();
        if (getSuperstep() == 0) {
            if (getVertexId() == start_id)
                current = 0;
            else
                current = MAX;
        } else {
            for (; !pmsgs->done(); pmsgs->next()) {
                if (pmsgs->getValue() < current) {
                    current = pmsgs->getValue();
                }
            }
        }

        // if self value has been changed, notify all neighbors
        double diff = getValue() - current;
        accumulateAggr(0, &diff);
        if (diff != 0 || getSuperstep() == 0) {
            *mutableValue() = current;
            OutEdgeIterator outEdgeIterator = getOutEdgeIterator();
            for (; !outEdgeIterator.done(); outEdgeIterator.next()) {

                double distance = outEdgeIterator.getValue() + current;
                sendMessageTo(outEdgeIterator.target(), distance);
            }
        }
    }
};

class VERTEX_CLASS_NAME(Graph) : public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator) *aggregator;

public:
    // argv[0]: PageRankVertex.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    void init(int argc, char *argv[]) {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 4) {
            printf("Usage: %s <input path> <output path> <start id>\n", argv[0]);
            exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];
        start_id = std::stoi(argv[3]);

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0, &aggregator[0]);
    }

    void term() {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph *create_graph() {
    Graph *pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph *pobject) {
    delete (VERTEX_CLASS_NAME() *) (pobject->m_pver_base);
    delete (VERTEX_CLASS_NAME(OutputFormatter) *) (pobject->m_pout_formatter);
    delete (VERTEX_CLASS_NAME(InputFormatter) *) (pobject->m_pin_formatter);
    delete (VERTEX_CLASS_NAME(Graph) *) pobject;
}


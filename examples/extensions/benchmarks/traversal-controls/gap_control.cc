// Thin driver over the unmodified, pinned GAPBS kernels. Include their source
// with renamed entry points so no algorithm implementation is copied here.
#define main upstream_bfs_main
#include "bfs.cc"
#undef main
#define main upstream_sssp_main
#include "sssp.cc"
#undef main
#include <chrono>
#include <fstream>
#include <iomanip>
#include <stdexcept>
#include <omp.h>

static int64_t integer(const char* text) {
  size_t consumed = 0;
  int64_t value = std::stoll(text, &consumed);
  if (text[consumed] || value < 0) throw std::runtime_error("invalid nonnegative integer");
  return value;
}

static void receipt(double seconds) {
  std::cout << std::setprecision(17) << "SAIL_CONTROL_RESULT {\"kernel_seconds\":" << seconds
            << ",\"thread_capacity\":" << omp_get_max_threads() << "}\n";
}

int main(int argc, char** argv) {
  try {
    if (argc < 6) throw std::runtime_error("usage: gap_control bfs|sssp INPUT SOURCE OUTPUT PARAM [BETA]");
    const std::string method(argv[1]);
    int64_t source = integer(argv[3]);
    int64_t param = integer(argv[5]);
    if (source > INT32_MAX || param < 1 || param > INT32_MAX) throw std::runtime_error("parameter out of range");
    std::ofstream output(argv[4]);
    if (!output) throw std::runtime_error("cannot create output");
    output << "id\tdistance\tparent\n";
    using clock = std::chrono::steady_clock;
    if (method == "bfs") {
      if (argc != 7) throw std::runtime_error("BFS requires alpha and beta");
      int64_t beta = integer(argv[6]);
      if (beta < 1 || beta > INT32_MAX) throw std::runtime_error("invalid beta");
      auto graph = Reader<NodeID>(argv[2]).ReadSerializedGraph();
      if (source >= graph.num_nodes()) throw std::runtime_error("source outside graph");
      auto start = clock::now();
      auto parents = DOBFS(graph, source, false, param, beta);
      double seconds = std::chrono::duration<double>(clock::now() - start).count();
      // Derive depths from the returned parent forest outside the kernel timer.
      // Each vertex is finalized once; malformed parents or cycles are errors.
      std::vector<int64_t> depths(graph.num_nodes(), -1);
      std::vector<bool> visiting(graph.num_nodes(), false);
      if (parents[source] != source) throw std::runtime_error("invalid root parent");
      depths[source] = 0;
      for (int64_t vertex = 0; vertex < graph.num_nodes(); ++vertex) {
        if (parents[vertex] < 0 || depths[vertex] >= 0) continue;
        std::vector<int64_t> path;
        int64_t current = vertex;
        while (depths[current] < 0) {
          if (visiting[current]) throw std::runtime_error("cycle in parent forest");
          visiting[current] = true;
          path.push_back(current);
          current = parents[current];
          if (current < 0 || current >= graph.num_nodes()) throw std::runtime_error("invalid parent");
        }
        while (!path.empty()) {
          int64_t node = path.back(); path.pop_back();
          depths[node] = depths[parents[node]] + 1;
          visiting[node] = false;
        }
      }
      for (int64_t node = 0; node < graph.num_nodes(); ++node) {
        output << node << '\t';
        if (parents[node] >= 0) output << depths[node] << '\t' << parents[node];
        else output << '\t';
        output << '\n';
      }
      receipt(seconds);
    } else if (method == "sssp") {
      if (argc != 6) throw std::runtime_error("SSSP requires delta");
      auto graph = Reader<NodeID, WNode, WeightT>(argv[2]).ReadSerializedGraph();
      if (source >= graph.num_nodes() || graph.num_edges_directed() == 0) throw std::runtime_error("invalid graph/source");
      auto start = clock::now();
      auto distances = DeltaStep(graph, source, param, false);
      double seconds = std::chrono::duration<double>(clock::now() - start).count();
      for (int64_t node = 0; node < graph.num_nodes(); ++node) {
        output << node << '\t';
        if (distances[node] != kDistInf) output << distances[node];
        output << "\t\n";
      }
      receipt(seconds);
    } else throw std::runtime_error("unknown method");
    output.close();
    if (!output) throw std::runtime_error("writing output failed");
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 2;
  }
}

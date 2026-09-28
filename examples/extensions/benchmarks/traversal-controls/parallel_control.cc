// Fixed-source driver over the unmodified pinned Parallel-SSSP kernels.
#include "sssp.h"
#include <chrono>
#include <fstream>
#include <iomanip>
#include <stdexcept>

static uint64_t integer(const char* text) {
  size_t consumed = 0;
  uint64_t value = std::stoull(text, &consumed);
  if (text[consumed] || text[0] == '-') throw std::runtime_error("invalid unsigned integer");
  return value;
}

template<class Solver, class... Args>
static auto execute(const Graph& graph, NodeId source, Args... args) {
  Solver solver(graph, args...);
  // Upstream's m/n setting is zero on graphs with average degree < 1.
  // Keep n/scale >= 1 for dense multigraphs too. Only the driver's tuning
  // parameter is clamped; the kernel source remains unchanged.
  solver.set_sd_scale(std::min(graph.n, std::max(size_t(1), graph.m / graph.n)));
  return solver.sssp(source);
}

int main(int argc, char** argv) {
  try {
    if (argc != 6) throw std::runtime_error("usage: parallel_control rho|delta|bellman-ford INPUT SOURCE OUTPUT PARAM");
    std::string method(argv[1]);
    uint64_t source = integer(argv[3]), param = integer(argv[5]);
    if (source > UINT32_MAX || param < 1 || param > UINT32_MAX) throw std::runtime_error("parameter out of range");
    Graph graph(true, false);  // All input files encode explicit directed arcs.
    graph.read_gapbs_format(argv[2]);
    if (!graph.n || source >= graph.n) throw std::runtime_error("source outside graph");
    std::ofstream output(argv[4]);
    if (!output) throw std::runtime_error("cannot create output");
    output << "id\tdistance\tparent\n";
    using clock = std::chrono::steady_clock;
    auto start = clock::now();
    sequence<EdgeTy> distances;
    if (method == "rho") distances = execute<Rho_Stepping>(graph, source, param);
    else if (method == "delta") distances = execute<Delta_Stepping>(graph, source, param);
    else if (method == "bellman-ford") distances = execute<Bellman_Ford>(graph, source);
    else throw std::runtime_error("unknown method");
    double seconds = std::chrono::duration<double>(clock::now() - start).count();
    for (size_t node = 0; node < graph.n; ++node) {
      output << node << '\t';
      if (distances[node] != DIST_MAX) output << distances[node];
      output << "\t\n";
    }
    output.close();
    if (!output) throw std::runtime_error("writing output failed");
    std::cout << std::setprecision(17) << "SAIL_CONTROL_RESULT {\"kernel_seconds\":" << seconds
              << ",\"thread_capacity\":" << parlay::num_workers()
              << ",\"sparse_dense_scale\":" << std::min(graph.n, std::max(size_t(1), graph.m / graph.n)) << "}\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 2;
  }
}

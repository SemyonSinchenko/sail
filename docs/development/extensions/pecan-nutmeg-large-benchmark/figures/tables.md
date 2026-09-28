# Large graph results

Median [minimum, maximum]; n is the number of successful samples. Missing measurements are unavailable. All outcomes remain in the CSV.

## hub-2097152

| Algorithm | Method | Path | Outcomes | Seconds | Sampled peak PSS GiB |
| --- | --- | --- | --- | --- | --- |
| pagerank | Power | Pecan | 3 passed | 40.992 [39.803, 43.044] (n=3) | 2.413 [2.344, 2.417] (n=3) |
| pagerank | Power | Banda | 3 passed | 23.664 [20.520, 24.580] (n=3) | 2.863 [2.836, 2.878] (n=3) |
| pagerank | Power | Grenada | 3 passed | 40.651 [38.525, 41.141] (n=3) | 2.436 [2.417, 2.663] (n=3) |
| pagerank | Delta/frontier | Pecan | 3 passed | 48.046 [47.579, 49.840] (n=3) | 2.523 [2.465, 2.539] (n=3) |
| pagerank | Delta/frontier | Banda | 3 passed | 27.639 [27.250, 28.744] (n=3) | 3.234 [3.234, 3.267] (n=3) |
| pagerank | Delta/frontier | Grenada | 3 passed | 48.537 [48.195, 48.595] (n=3) | 2.502 [2.489, 2.548] (n=3) |
| wcc | Reference | Pecan | 3 passed | 26.755 [25.977, 27.634] (n=3) | 3.701 [3.683, 4.080] (n=3) |
| wcc | Reference | Banda | 3 passed | 20.123 [19.251, 25.499] (n=3) | 2.838 [2.690, 2.953] (n=3) |
| wcc | Reference | Grenada | 3 passed | 27.049 [26.462, 27.445] (n=3) | 3.953 [3.697, 4.000] (n=3) |
| wcc | Randomized | Pecan | 3 passed | 24.667 [24.042, 25.863] (n=3) | 2.961 [2.910, 2.976] (n=3) |
| wcc | Randomized | Banda | 3 passed | 26.332 [25.798, 30.463] (n=3) | 3.026 [3.025, 3.069] (n=3) |
| wcc | Randomized | Grenada | 3 passed | 24.710 [24.175, 25.179] (n=3) | 2.872 [2.857, 2.881] (n=3) |
| wcc | Fused randomized | Pecan | 3 passed | 20.965 [20.645, 24.935] (n=3) | 3.145 [3.119, 3.370] (n=3) |
| wcc | Fused randomized | Banda | 3 passed | 30.305 [26.396, 32.975] (n=3) | 3.042 [3.036, 3.058] (n=3) |
| wcc | Fused randomized | Grenada | 3 passed | 22.052 [21.635, 23.047] (n=3) | 3.515 [3.094, 3.586] (n=3) |

![Time and memory for hub-2097152](hub-2097152.png)

## uniform-2097152

| Algorithm | Method | Path | Outcomes | Seconds | Sampled peak PSS GiB |
| --- | --- | --- | --- | --- | --- |
| pagerank | Power | Pecan | 3 passed | 41.318 [39.825, 42.804] (n=3) | 2.454 [2.427, 2.497] (n=3) |
| pagerank | Power | Banda | 3 passed | 21.080 [19.638, 23.136] (n=3) | 3.022 [2.835, 3.053] (n=3) |
| pagerank | Power | Grenada | 3 passed | 39.370 [38.399, 40.002] (n=3) | 2.449 [2.444, 2.509] (n=3) |
| pagerank | Delta/frontier | Pecan | 3 passed | 46.326 [44.550, 46.737] (n=3) | 2.639 [2.592, 2.645] (n=3) |
| pagerank | Delta/frontier | Banda | 3 passed | 28.067 [27.482, 31.685] (n=3) | 3.240 [3.216, 3.284] (n=3) |
| pagerank | Delta/frontier | Grenada | 3 passed | 46.104 [45.009, 46.201] (n=3) | 2.617 [2.592, 2.619] (n=3) |
| wcc | Reference | Pecan | 2 passed, 1 timeout | 32.819 [32.529, 33.109] (n=2) | 3.863 [3.831, 3.894] (n=2) |
| wcc | Reference | Banda | 3 passed | 19.883 [18.886, 20.236] (n=3) | 3.017 [2.983, 3.034] (n=3) |
| wcc | Reference | Grenada | 3 passed | 33.154 [31.766, 39.526] (n=3) | 4.115 [3.760, 4.357] (n=3) |
| wcc | Randomized | Pecan | 3 passed | 29.164 [25.265, 29.778] (n=3) | 2.929 [2.811, 2.934] (n=3) |
| wcc | Randomized | Banda | 3 passed | 26.496 [25.099, 31.394] (n=3) | 3.060 [3.040, 3.075] (n=3) |
| wcc | Randomized | Grenada | 3 passed | 25.673 [25.032, 25.999] (n=3) | 2.856 [2.712, 2.996] (n=3) |
| wcc | Fused randomized | Pecan | 3 passed | 21.990 [21.753, 24.597] (n=3) | 3.168 [3.124, 3.174] (n=3) |
| wcc | Fused randomized | Banda | 3 passed | 24.623 [24.301, 26.161] (n=3) | 3.030 [3.028, 3.055] (n=3) |
| wcc | Fused randomized | Grenada | 3 passed | 22.127 [20.954, 26.367] (n=3) | 3.218 [3.126, 3.268] (n=3) |

![Time and memory for uniform-2097152](uniform-2097152.png)

## hub-4194304

| Algorithm | Method | Path | Outcomes | Seconds | Sampled peak PSS GiB |
| --- | --- | --- | --- | --- | --- |
| pagerank | Power | Pecan | 3 passed | 83.353 [80.350, 83.480] (n=3) | 3.348 [3.345, 3.420] (n=3) |
| pagerank | Power | Banda | 3 passed | 44.827 [43.296, 54.059] (n=3) | 4.698 [4.688, 4.728] (n=3) |
| pagerank | Power | Grenada | 3 passed | 83.524 [81.019, 87.333] (n=3) | 3.353 [3.189, 3.397] (n=3) |
| pagerank | Delta/frontier | Pecan | 3 passed | 94.931 [93.679, 95.014] (n=3) | 3.248 [3.243, 3.281] (n=3) |
| pagerank | Delta/frontier | Banda | 3 passed | 67.523 [66.526, 69.884] (n=3) | 5.283 [5.251, 5.306] (n=3) |
| pagerank | Delta/frontier | Grenada | 3 passed | 92.023 [90.866, 93.662] (n=3) | 3.362 [3.311, 3.419] (n=3) |
| wcc | Reference | Pecan | 3 passed | 50.953 [49.844, 63.095] (n=3) | 5.838 [5.282, 6.619] (n=3) |
| wcc | Reference | Banda | 3 passed | 50.563 [43.848, 50.747] (n=3) | 4.442 [4.399, 4.446] (n=3) |
| wcc | Reference | Grenada | 3 passed | 58.900 [51.155, 89.989] (n=3) | 6.187 [5.711, 6.338] (n=3) |
| wcc | Randomized | Pecan | 3 passed | 49.131 [49.091, 50.367] (n=3) | 4.019 [3.979, 4.143] (n=3) |
| wcc | Randomized | Banda | 3 passed | 58.738 [58.100, 60.728] (n=3) | 5.170 [5.108, 5.188] (n=3) |
| wcc | Randomized | Grenada | 3 passed | 48.338 [47.842, 50.861] (n=3) | 4.307 [4.084, 4.338] (n=3) |
| wcc | Fused randomized | Pecan | 3 passed | 40.822 [40.205, 40.896] (n=3) | 4.908 [4.860, 4.933] (n=3) |
| wcc | Fused randomized | Banda | 3 passed | 64.087 [56.957, 64.102] (n=3) | 5.106 [5.060, 5.131] (n=3) |
| wcc | Fused randomized | Grenada | 3 passed | 41.722 [39.759, 43.107] (n=3) | 5.340 [4.937, 5.592] (n=3) |

![Time and memory for hub-4194304](hub-4194304.png)

## uniform-4194304

| Algorithm | Method | Path | Outcomes | Seconds | Sampled peak PSS GiB |
| --- | --- | --- | --- | --- | --- |
| pagerank | Power | Pecan | 3 passed | 81.479 [81.103, 82.278] (n=3) | 3.354 [3.281, 3.569] (n=3) |
| pagerank | Power | Banda | 3 passed | 43.429 [43.153, 44.907] (n=3) | 4.669 [4.636, 4.686] (n=3) |
| pagerank | Power | Grenada | 3 passed | 80.245 [79.519, 83.024] (n=3) | 3.575 [3.296, 3.659] (n=3) |
| pagerank | Delta/frontier | Pecan | 3 passed | 85.876 [83.772, 87.933] (n=3) | 3.338 [3.333, 3.454] (n=3) |
| pagerank | Delta/frontier | Banda | 3 passed | 65.893 [65.211, 66.700] (n=3) | 5.283 [5.263, 5.287] (n=3) |
| pagerank | Delta/frontier | Grenada | 3 passed | 87.783 [87.648, 88.415] (n=3) | 3.366 [3.324, 3.370] (n=3) |
| wcc | Reference | Pecan | 3 passed | 62.018 [61.278, 71.842] (n=3) | 5.921 [5.892, 6.218] (n=3) |
| wcc | Reference | Banda | 3 passed | 50.225 [41.569, 54.476] (n=3) | 4.397 [4.390, 4.398] (n=3) |
| wcc | Reference | Grenada | 3 passed | 62.760 [62.013, 62.841] (n=3) | 6.216 [5.741, 6.487] (n=3) |
| wcc | Randomized | Pecan | 3 passed | 52.485 [50.824, 52.983] (n=3) | 4.337 [4.304, 4.337] (n=3) |
| wcc | Randomized | Banda | 3 passed | 58.400 [58.185, 58.564] (n=3) | 5.129 [5.093, 5.158] (n=3) |
| wcc | Randomized | Grenada | 3 passed | 51.952 [48.216, 58.897] (n=3) | 4.247 [3.926, 4.409] (n=3) |
| wcc | Fused randomized | Pecan | 3 passed | 40.623 [40.119, 42.203] (n=3) | 4.601 [4.573, 4.836] (n=3) |
| wcc | Fused randomized | Banda | 3 passed | 62.804 [54.237, 63.235] (n=3) | 5.133 [5.045, 5.143] (n=3) |
| wcc | Fused randomized | Grenada | 3 passed | 41.477 [40.429, 41.748] (n=3) | 4.582 [4.558, 4.767] (n=3) |

![Time and memory for uniform-4194304](uniform-4194304.png)


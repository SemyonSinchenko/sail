# Independent large evidence audit

Recorded 2026-09-28T03:05:22.343670+00:00. Audit verdict: **passed**.

180 planned cells; outcomes: 179 passed, 1 timeout.

Original trial outcomes are retained unchanged. An audit failure is a separate evidence-integrity finding, not a rewritten benchmark outcome.

## Identity and coverage

- harness_source_sha: `aa4b5fa6bd1a8e0ac833ec9db0862386777575c2`.
- runtime_source_sha: `70b0d1cab2cab945d4dbaf6842ee0e38c8aa1822`.
- native_source_sha: `9d7155aa2302d9dda4e93e48ee3fd81aac1ee637`.

Performed 3286 inventory checks across 1830 unique files; reconstructed 117958 raw memory samples.
Raw correctness coverage: 179 raw_full_vector, 1 receipt_only.

Raw-vector checks read actual Parquet and independently recompute PageRank fixed-point residuals/full-vector errors and WCC components from input edges. Receipt-only checks validate recorded results and hashes; they are not a new calculation from omitted graph rows.

## Findings

No audit integrity defects found.

## Every planned outcome

| Sequence | Cell | Outcome | Correctness audit | Execution samples |
| --- | --- | --- | --- | ---: |
| 1 | `large-pagerank-r1-uniform-4194304-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 496 |
| 2 | `large-pagerank-r1-uniform-2097152-pecan-pagerank-reference` | passed | raw_full_vector | 418 |
| 3 | `large-pagerank-r1-uniform-2097152-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 345 |
| 4 | `large-pagerank-r1-hub-2097152-nutmeg-datafusion-pagerank-optimized` | passed | raw_full_vector | 480 |
| 5 | `large-pagerank-r1-hub-4194304-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 515 |
| 6 | `large-pagerank-r1-hub-4194304-nutmeg-datafusion-pagerank-optimized` | passed | raw_full_vector | 813 |
| 7 | `large-pagerank-r1-hub-2097152-pecan-pagerank-reference` | passed | raw_full_vector | 414 |
| 8 | `large-pagerank-r1-uniform-2097152-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 252 |
| 9 | `large-pagerank-r1-hub-4194304-nutmeg-datafusion-pagerank-reference` | passed | raw_full_vector | 796 |
| 10 | `large-pagerank-r1-uniform-4194304-nutmeg-datafusion-pagerank-optimized` | passed | raw_full_vector | 767 |
| 11 | `large-pagerank-r1-uniform-4194304-pecan-pagerank-reference` | passed | raw_full_vector | 731 |
| 12 | `large-pagerank-r1-hub-4194304-pecan-pagerank-optimized` | passed | raw_full_vector | 803 |
| 13 | `large-pagerank-r1-hub-2097152-nutmeg-datafusion-pagerank-reference` | passed | raw_full_vector | 415 |
| 14 | `large-pagerank-r1-hub-2097152-pecan-pagerank-optimized` | passed | raw_full_vector | 471 |
| 15 | `large-pagerank-r1-uniform-4194304-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 685 |
| 16 | `large-pagerank-r1-uniform-2097152-nutmeg-datafusion-pagerank-optimized` | passed | raw_full_vector | 440 |
| 17 | `large-pagerank-r1-hub-4194304-pecan-pagerank-reference` | passed | raw_full_vector | 727 |
| 18 | `large-pagerank-r1-uniform-4194304-nutmeg-datafusion-pagerank-reference` | passed | raw_full_vector | 711 |
| 19 | `large-pagerank-r1-uniform-4194304-pecan-pagerank-optimized` | passed | raw_full_vector | 742 |
| 20 | `large-pagerank-r1-hub-2097152-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 303 |
| 21 | `large-pagerank-r1-hub-2097152-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 331 |
| 22 | `large-pagerank-r1-uniform-2097152-nutmeg-datafusion-pagerank-reference` | passed | raw_full_vector | 393 |
| 23 | `large-pagerank-r1-hub-4194304-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 695 |
| 24 | `large-pagerank-r1-uniform-2097152-pecan-pagerank-optimized` | passed | raw_full_vector | 424 |
| 25 | `large-pagerank-r2-hub-2097152-nutmeg-datafusion-pagerank-optimized` | passed | raw_full_vector | 465 |
| 26 | `large-pagerank-r2-uniform-2097152-nutmeg-datafusion-pagerank-reference` | passed | raw_full_vector | 377 |
| 27 | `large-pagerank-r2-uniform-4194304-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 684 |
| 28 | `large-pagerank-r2-hub-4194304-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 479 |
| 29 | `large-pagerank-r2-hub-4194304-pecan-pagerank-reference` | passed | raw_full_vector | 753 |
| 30 | `large-pagerank-r2-uniform-2097152-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 291 |
| 31 | `large-pagerank-r2-uniform-2097152-pecan-pagerank-optimized` | passed | raw_full_vector | 441 |
| 32 | `large-pagerank-r2-uniform-4194304-nutmeg-datafusion-pagerank-reference` | passed | raw_full_vector | 706 |
| 33 | `large-pagerank-r2-uniform-4194304-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 464 |
| 34 | `large-pagerank-r2-uniform-2097152-nutmeg-datafusion-pagerank-optimized` | passed | raw_full_vector | 424 |
| 35 | `large-pagerank-r2-uniform-4194304-pecan-pagerank-reference` | passed | raw_full_vector | 703 |
| 36 | `large-pagerank-r2-hub-4194304-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 692 |
| 37 | `large-pagerank-r2-uniform-4194304-pecan-pagerank-optimized` | passed | raw_full_vector | 717 |
| 38 | `large-pagerank-r2-hub-2097152-pecan-pagerank-reference` | passed | raw_full_vector | 393 |
| 39 | `large-pagerank-r2-hub-2097152-pecan-pagerank-optimized` | passed | raw_full_vector | 456 |
| 40 | `large-pagerank-r2-hub-4194304-nutmeg-datafusion-pagerank-optimized` | passed | raw_full_vector | 777 |
| 41 | `large-pagerank-r2-hub-2097152-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 323 |
| 42 | `large-pagerank-r2-hub-4194304-pecan-pagerank-optimized` | passed | raw_full_vector | 809 |
| 43 | `large-pagerank-r2-uniform-2097152-pecan-pagerank-reference` | passed | raw_full_vector | 384 |
| 44 | `large-pagerank-r2-uniform-2097152-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 374 |
| 45 | `large-pagerank-r2-hub-2097152-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 304 |
| 46 | `large-pagerank-r2-uniform-4194304-nutmeg-datafusion-pagerank-optimized` | passed | raw_full_vector | 744 |
| 47 | `large-pagerank-r2-hub-2097152-nutmeg-datafusion-pagerank-reference` | passed | raw_full_vector | 394 |
| 48 | `large-pagerank-r2-hub-4194304-nutmeg-datafusion-pagerank-reference` | passed | raw_full_vector | 720 |
| 49 | `large-pagerank-r3-uniform-4194304-pecan-pagerank-optimized` | passed | raw_full_vector | 748 |
| 50 | `large-pagerank-r3-hub-4194304-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 703 |
| 51 | `large-pagerank-r3-hub-4194304-nutmeg-datafusion-pagerank-reference` | passed | raw_full_vector | 740 |
| 52 | `large-pagerank-r3-hub-4194304-pecan-pagerank-optimized` | passed | raw_full_vector | 791 |
| 53 | `large-pagerank-r3-uniform-2097152-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 256 |
| 54 | `large-pagerank-r3-uniform-4194304-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 471 |
| 55 | `large-pagerank-r3-uniform-2097152-nutmeg-datafusion-pagerank-reference` | passed | raw_full_vector | 387 |
| 56 | `large-pagerank-r3-uniform-2097152-nutmeg-datafusion-pagerank-optimized` | passed | raw_full_vector | 427 |
| 57 | `large-pagerank-r3-uniform-4194304-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 672 |
| 58 | `large-pagerank-r3-hub-4194304-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 583 |
| 59 | `large-pagerank-r3-hub-4194304-pecan-pagerank-reference` | passed | raw_full_vector | 731 |
| 60 | `large-pagerank-r3-hub-4194304-nutmeg-datafusion-pagerank-optimized` | passed | raw_full_vector | 786 |
| 61 | `large-pagerank-r3-uniform-2097152-pecan-pagerank-optimized` | passed | raw_full_vector | 435 |
| 62 | `large-pagerank-r3-uniform-2097152-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 328 |
| 63 | `large-pagerank-r3-hub-2097152-pecan-pagerank-reference` | passed | raw_full_vector | 418 |
| 64 | `large-pagerank-r3-hub-2097152-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 252 |
| 65 | `large-pagerank-r3-hub-2097152-nutmeg-datafusion-pagerank-optimized` | passed | raw_full_vector | 463 |
| 66 | `large-pagerank-r3-uniform-4194304-nutmeg-datafusion-pagerank-optimized` | passed | raw_full_vector | 742 |
| 67 | `large-pagerank-r3-hub-2097152-pecan-pagerank-optimized` | passed | raw_full_vector | 467 |
| 68 | `large-pagerank-r3-uniform-2097152-pecan-pagerank-reference` | passed | raw_full_vector | 401 |
| 69 | `large-pagerank-r3-hub-2097152-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 339 |
| 70 | `large-pagerank-r3-uniform-4194304-nutmeg-datafusion-pagerank-reference` | passed | raw_full_vector | 727 |
| 71 | `large-pagerank-r3-hub-2097152-nutmeg-datafusion-pagerank-reference` | passed | raw_full_vector | 383 |
| 72 | `large-pagerank-r3-uniform-4194304-pecan-pagerank-reference` | passed | raw_full_vector | 719 |
| 73 | `large-wcc-r1-hub-2097152-nutmeg-datafusion-wcc-fused` | passed | raw_full_vector | 217 |
| 74 | `large-wcc-r1-uniform-4194304-nutmeg-native-wcc-fused` | passed | raw_full_vector | 663 |
| 75 | `large-wcc-r1-hub-4194304-nutmeg-datafusion-wcc-fused` | passed | raw_full_vector | 375 |
| 76 | `large-wcc-r1-uniform-4194304-pecan-wcc-optimized` | passed | raw_full_vector | 483 |
| 77 | `large-wcc-r1-uniform-4194304-pecan-wcc-fused` | passed | raw_full_vector | 366 |
| 78 | `large-wcc-r1-hub-4194304-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 460 |
| 79 | `large-wcc-r1-uniform-2097152-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 261 |
| 80 | `large-wcc-r1-uniform-2097152-pecan-wcc-reference` | passed | raw_full_vector | 303 |
| 81 | `large-wcc-r1-hub-2097152-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 257 |
| 82 | `large-wcc-r1-hub-2097152-nutmeg-datafusion-wcc-reference` | passed | raw_full_vector | 258 |
| 83 | `large-wcc-r1-uniform-4194304-pecan-wcc-reference` | passed | raw_full_vector | 516 |
| 84 | `large-wcc-r1-hub-2097152-pecan-wcc-optimized` | passed | raw_full_vector | 269 |
| 85 | `large-wcc-r1-hub-2097152-pecan-wcc-reference` | passed | raw_full_vector | 252 |
| 86 | `large-wcc-r1-uniform-4194304-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 597 |
| 87 | `large-wcc-r1-hub-4194304-pecan-wcc-optimized` | passed | raw_full_vector | 456 |
| 88 | `large-wcc-r1-uniform-4194304-nutmeg-datafusion-wcc-fused` | passed | raw_full_vector | 387 |
| 89 | `large-wcc-r1-hub-4194304-nutmeg-native-wcc-fused` | passed | raw_full_vector | 679 |
| 90 | `large-wcc-r1-hub-4194304-pecan-wcc-fused` | passed | raw_full_vector | 357 |
| 91 | `large-wcc-r1-hub-4194304-nutmeg-native-wcc-reference` | passed | raw_full_vector | 564 |
| 92 | `large-wcc-r1-uniform-2097152-nutmeg-native-wcc-reference` | passed | raw_full_vector | 238 |
| 93 | `large-wcc-r1-uniform-2097152-nutmeg-native-wcc-fused` | passed | raw_full_vector | 294 |
| 94 | `large-wcc-r1-uniform-2097152-pecan-wcc-optimized` | passed | raw_full_vector | 297 |
| 95 | `large-wcc-r1-hub-2097152-nutmeg-native-wcc-reference` | passed | raw_full_vector | 288 |
| 96 | `large-wcc-r1-hub-2097152-pecan-wcc-fused` | passed | raw_full_vector | 238 |
| 97 | `large-wcc-r1-hub-2097152-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 349 |
| 98 | `large-wcc-r1-uniform-4194304-nutmeg-datafusion-wcc-reference` | passed | raw_full_vector | 528 |
| 99 | `large-wcc-r1-hub-4194304-nutmeg-datafusion-wcc-reference` | passed | raw_full_vector | 597 |
| 100 | `large-wcc-r1-hub-4194304-pecan-wcc-reference` | passed | raw_full_vector | 509 |
| 101 | `large-wcc-r1-uniform-2097152-pecan-wcc-fused` | passed | raw_full_vector | 243 |
| 102 | `large-wcc-r1-hub-2097152-nutmeg-native-wcc-fused` | passed | raw_full_vector | 312 |
| 103 | `large-wcc-r1-hub-4194304-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 626 |
| 104 | `large-wcc-r1-uniform-2097152-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 355 |
| 105 | `large-wcc-r1-uniform-2097152-nutmeg-datafusion-wcc-fused` | passed | raw_full_vector | 256 |
| 106 | `large-wcc-r1-uniform-4194304-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 539 |
| 107 | `large-wcc-r1-uniform-4194304-nutmeg-native-wcc-reference` | passed | raw_full_vector | 584 |
| 108 | `large-wcc-r1-uniform-2097152-nutmeg-datafusion-wcc-reference` | passed | raw_full_vector | 340 |
| 109 | `large-wcc-r2-uniform-2097152-pecan-wcc-optimized` | passed | raw_full_vector | 301 |
| 110 | `large-wcc-r2-uniform-4194304-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 601 |
| 111 | `large-wcc-r2-hub-2097152-nutmeg-native-wcc-fused` | passed | raw_full_vector | 388 |
| 112 | `large-wcc-r2-hub-4194304-nutmeg-datafusion-wcc-reference` | passed | raw_full_vector | 499 |
| 113 | `large-wcc-r2-uniform-4194304-pecan-wcc-reference` | passed | raw_full_vector | 581 |
| 114 | `large-wcc-r2-hub-4194304-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 608 |
| 115 | `large-wcc-r2-uniform-4194304-nutmeg-native-wcc-reference` | passed | raw_full_vector | 551 |
| 116 | `large-wcc-r2-uniform-2097152-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 300 |
| 117 | `large-wcc-r2-hub-2097152-pecan-wcc-reference` | passed | raw_full_vector | 244 |
| 118 | `large-wcc-r2-uniform-2097152-nutmeg-datafusion-wcc-reference` | passed | raw_full_vector | 292 |
| 119 | `large-wcc-r2-hub-4194304-nutmeg-datafusion-wcc-fused` | passed | raw_full_vector | 360 |
| 120 | `large-wcc-r2-hub-2097152-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 259 |
| 121 | `large-wcc-r2-hub-2097152-nutmeg-native-wcc-reference` | passed | raw_full_vector | 241 |
| 122 | `large-wcc-r2-uniform-4194304-nutmeg-datafusion-wcc-fused` | passed | raw_full_vector | 369 |
| 123 | `large-wcc-r2-hub-2097152-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 308 |
| 124 | `large-wcc-r2-uniform-4194304-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 460 |
| 125 | `large-wcc-r2-uniform-4194304-nutmeg-native-wcc-fused` | passed | raw_full_vector | 668 |
| 126 | `large-wcc-r2-uniform-2097152-nutmeg-native-wcc-fused` | passed | raw_full_vector | 295 |
| 127 | `large-wcc-r2-hub-4194304-nutmeg-native-wcc-reference` | passed | raw_full_vector | 563 |
| 128 | `large-wcc-r2-uniform-2097152-nutmeg-datafusion-wcc-fused` | passed | raw_full_vector | 213 |
| 129 | `large-wcc-r2-uniform-2097152-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 271 |
| 130 | `large-wcc-r2-uniform-4194304-nutmeg-datafusion-wcc-reference` | passed | raw_full_vector | 525 |
| 131 | `large-wcc-r2-hub-4194304-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 478 |
| 132 | `large-wcc-r2-uniform-2097152-nutmeg-native-wcc-reference` | passed | raw_full_vector | 242 |
| 133 | `large-wcc-r2-uniform-2097152-pecan-wcc-reference` | timeout | receipt_only | 26708 |
| 134 | `large-wcc-r2-hub-2097152-pecan-wcc-optimized` | passed | raw_full_vector | 263 |
| 135 | `large-wcc-r2-hub-4194304-pecan-wcc-reference` | passed | raw_full_vector | 444 |
| 136 | `large-wcc-r2-hub-4194304-pecan-wcc-fused` | passed | raw_full_vector | 369 |
| 137 | `large-wcc-r2-hub-4194304-pecan-wcc-optimized` | passed | raw_full_vector | 466 |
| 138 | `large-wcc-r2-uniform-2097152-pecan-wcc-fused` | passed | raw_full_vector | 222 |
| 139 | `large-wcc-r2-hub-2097152-pecan-wcc-fused` | passed | raw_full_vector | 209 |
| 140 | `large-wcc-r2-hub-2097152-nutmeg-datafusion-wcc-fused` | passed | raw_full_vector | 223 |
| 141 | `large-wcc-r2-uniform-4194304-pecan-wcc-optimized` | passed | raw_full_vector | 497 |
| 142 | `large-wcc-r2-hub-4194304-nutmeg-native-wcc-fused` | passed | raw_full_vector | 686 |
| 143 | `large-wcc-r2-uniform-4194304-pecan-wcc-fused` | passed | raw_full_vector | 387 |
| 144 | `large-wcc-r2-hub-2097152-nutmeg-datafusion-wcc-reference` | passed | raw_full_vector | 259 |
| 145 | `large-wcc-r3-hub-4194304-pecan-wcc-reference` | passed | raw_full_vector | 431 |
| 146 | `large-wcc-r3-uniform-2097152-nutmeg-native-wcc-reference` | passed | raw_full_vector | 249 |
| 147 | `large-wcc-r3-uniform-4194304-nutmeg-datafusion-wcc-fused` | passed | raw_full_vector | 378 |
| 148 | `large-wcc-r3-uniform-2097152-nutmeg-native-wcc-fused` | passed | raw_full_vector | 310 |
| 149 | `large-wcc-r3-hub-4194304-nutmeg-native-wcc-fused` | passed | raw_full_vector | 590 |
| 150 | `large-wcc-r3-uniform-4194304-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 488 |
| 151 | `large-wcc-r3-hub-4194304-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 608 |
| 152 | `large-wcc-r3-hub-2097152-nutmeg-datafusion-wcc-reference` | passed | raw_full_vector | 256 |
| 153 | `large-wcc-r3-hub-2097152-nutmeg-datafusion-wcc-fused` | passed | raw_full_vector | 212 |
| 154 | `large-wcc-r3-hub-4194304-nutmeg-datafusion-wcc-fused` | passed | raw_full_vector | 383 |
| 155 | `large-wcc-r3-uniform-4194304-nutmeg-datafusion-wcc-reference` | passed | raw_full_vector | 513 |
| 156 | `large-wcc-r3-uniform-2097152-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 312 |
| 157 | `large-wcc-r3-hub-2097152-pecan-wcc-fused` | passed | raw_full_vector | 212 |
| 158 | `large-wcc-r3-uniform-4194304-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 607 |
| 159 | `large-wcc-r3-hub-2097152-pecan-wcc-reference` | passed | raw_full_vector | 251 |
| 160 | `large-wcc-r3-hub-4194304-pecan-wcc-optimized` | passed | raw_full_vector | 469 |
| 161 | `large-wcc-r3-uniform-4194304-pecan-wcc-optimized` | passed | raw_full_vector | 488 |
| 162 | `large-wcc-r3-hub-2097152-nutmeg-native-wcc-reference` | passed | raw_full_vector | 249 |
| 163 | `large-wcc-r3-hub-2097152-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 263 |
| 164 | `large-wcc-r3-hub-2097152-pecan-wcc-optimized` | passed | raw_full_vector | 253 |
| 165 | `large-wcc-r3-hub-4194304-nutmeg-datafusion-wcc-reference` | passed | raw_full_vector | 437 |
| 166 | `large-wcc-r3-uniform-2097152-pecan-wcc-fused` | passed | raw_full_vector | 215 |
| 167 | `large-wcc-r3-hub-4194304-nutmeg-native-wcc-reference` | passed | raw_full_vector | 479 |
| 168 | `large-wcc-r3-uniform-2097152-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 267 |
| 169 | `large-wcc-r3-hub-4194304-pecan-wcc-fused` | passed | raw_full_vector | 368 |
| 170 | `large-wcc-r3-hub-2097152-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 313 |
| 171 | `large-wcc-r3-hub-2097152-nutmeg-native-wcc-fused` | passed | raw_full_vector | 364 |
| 172 | `large-wcc-r3-uniform-2097152-nutmeg-datafusion-wcc-fused` | passed | raw_full_vector | 222 |
| 173 | `large-wcc-r3-hub-4194304-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 446 |
| 174 | `large-wcc-r3-uniform-4194304-pecan-wcc-fused` | passed | raw_full_vector | 369 |
| 175 | `large-wcc-r3-uniform-2097152-pecan-wcc-optimized` | passed | raw_full_vector | 261 |
| 176 | `large-wcc-r3-uniform-4194304-pecan-wcc-reference` | passed | raw_full_vector | 513 |
| 177 | `large-wcc-r3-uniform-2097152-pecan-wcc-reference` | passed | raw_full_vector | 307 |
| 178 | `large-wcc-r3-uniform-4194304-nutmeg-native-wcc-fused` | passed | raw_full_vector | 565 |
| 179 | `large-wcc-r3-uniform-2097152-nutmeg-datafusion-wcc-reference` | passed | raw_full_vector | 304 |
| 180 | `large-wcc-r3-uniform-4194304-nutmeg-native-wcc-reference` | passed | raw_full_vector | 451 |

## Sampling and contraction detail

| Path | Algorithm | Variant | Execution sample count: min / median / max |
| --- | --- | --- | --- |
| nutmeg-datafusion | pagerank | optimized | 424 / 611.0 / 813 |
| nutmeg-datafusion | pagerank | reference | 377 / 560.5 / 796 |
| nutmeg-datafusion | wcc | fused | 212 / 308.0 / 387 |
| nutmeg-datafusion | wcc | optimized | 257 / 358.5 / 539 |
| nutmeg-datafusion | wcc | reference | 256 / 388.5 / 597 |
| nutmeg-native | pagerank | optimized | 323 / 523.0 / 703 |
| nutmeg-native | pagerank | reference | 252 / 384.0 / 583 |
| nutmeg-native | wcc | fused | 294 / 476.5 / 686 |
| nutmeg-native | wcc | optimized | 300 / 476.0 / 626 |
| nutmeg-native | wcc | reference | 238 / 369.5 / 584 |
| pecan | pagerank | optimized | 424 / 594.0 / 809 |
| pecan | pagerank | reference | 384 / 560.5 / 753 |
| pecan | wcc | fused | 209 / 300.0 / 387 |
| pecan | wcc | optimized | 253 / 378.5 / 497 |
| pecan | wcc | reference | 244 / 437.5 / 26708 |

Recorded whole-VM steal fractions: `{"observations": 180, "minimum": 0.0, "median": 0.0, "maximum": 0.0}`.

- `hub-2097152`: 11 identical contraction rounds across 18 successful cells, excluding the first raw-edge input count.
- `uniform-4194304`: 13 identical contraction rounds across 18 successful cells, excluding the first raw-edge input count.
- `hub-4194304`: 12 identical contraction rounds across 18 successful cells, excluding the first raw-edge input count.
- `uniform-2097152`: 12 identical contraction rounds across 18 successful cells, excluding the first raw-edge input count.

Native advanced widths are recorded per dataset in the JSON. Native reference actual widths are not exported by these kernels and must not be inferred from requested concurrency.

Post-stop cleanup snapshot: `{"recorded_utc": "2026-09-28T02:38:29.096396+00:00", "harness_source_sha": "aa4b5fa6bd1a8e0ac833ec9db0862386777575c2", "run": "/targets/pecan-benchmark/runs/large-qualified-full-aa4b5fa6", "completed_cell_directories": 180, "remaining_staging_regular_files": [], "scope": "Persistent container-volume cell directories after all trial containers stopped; no files removed"}`.


RSS/PSS are sampled execution-phase totals; short peaks may be missed. RSS can double-count shared mappings; PSS apportions them. Cgroup peaks include cache and lifetime activity and are not interchangeable with PSS or native admission accounting. No elapsed-time or memory ranking is inferred by this audit.

Cross-path WCC trace comparisons check coefficients, active vertices and contracted edges; only the first raw-edge input count may differ for fused plans. Floating PageRank frontier traces are recorded without requiring equality across different reduction orders.

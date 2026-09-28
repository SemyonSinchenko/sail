/* Adapter around the pinned Graph500 generator. No generator code is copied.
 * stdout: little-endian (int64 src, int64 dst, float64 weight) records.
 * Compile with -DSSSP and the upstream generator translation units.
 */
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "user_settings.h"
#include "graph_generator.h"
#include "utils.h"

typedef struct { int64_t src, dst; double weight; } record;
_Static_assert(sizeof(record)==24, "unsupported record layout");
static uint64_t number(const char* text) {
    char* end; errno=0;
    unsigned long long value=strtoull(text,&end,10);
    if(errno || !*text || *end || *text=='-') { fprintf(stderr,"invalid integer: %s\n",text);exit(2); }
    return (uint64_t)value;
}
int main(int argc,char** argv) {
    if(argc!=6) {fprintf(stderr,"usage: graph500_stream SCALE EDGEFACTOR SEED1 SEED2 CHUNK_EDGES\n");return 2;}
    uint64_t scale=number(argv[1]),factor=number(argv[2]),s1=number(argv[3]),s2=number(argv[4]),chunk=number(argv[5]);
    uint16_t endian=1;
    if(*(uint8_t*)&endian!=1 || scale<1 || scale>40 || factor<1 || factor>(INT64_MAX>>scale) || chunk<1 || chunk>16777216) {
        fprintf(stderr,"requires little endian, scale 1..40, positive bounded factor and chunk 1..16777216\n");return 2;
    }
    int64_t count=(int64_t)(factor<<scale);
    packed_edge* edges=malloc(chunk*sizeof(*edges));
    float* weights=malloc(chunk*sizeof(*weights));
    record* records=malloc(chunk*sizeof(*records));
    if(!edges||!weights||!records){fprintf(stderr,"allocation failed\n");return 3;}
    uint_fast32_t seed[5];make_mrg_seed(s1,s2,seed);
    for(int64_t start=0;start<count;) {
        int64_t length=count-start<(int64_t)chunk?count-start:(int64_t)chunk;
        generate_kronecker_range(seed,(int)scale,start,start+length,edges,weights);
        for(int64_t i=0;i<length;i++) {
            records[i].src=get_v0_from_edge(&edges[i]);records[i].dst=get_v1_from_edge(&edges[i]);records[i].weight=(double)weights[i];
        }
        if(fwrite(records,sizeof(*records),(size_t)length,stdout)!=(size_t)length){perror("write");return 4;}
        start+=length;
    }
    free(records);free(weights);free(edges);
    return fflush(stdout)==0?0:4;
}

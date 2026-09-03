#include <omp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_ROWS ((uint64_t)16777216)
#define MAX_ALLOWED_GRAPHS ((uint64_t)1048576)

struct row {
    uint64_t subject;
    uint64_t predicate;
    uint64_t object;
    uint64_t graph;
    uint8_t queryable;
};

static int read_exact(void *buffer, size_t bytes) {
    return fread(buffer, 1, bytes, stdin) == bytes ? 0 : -1;
}

static int write_exact(const void *buffer, size_t bytes) {
    return fwrite(buffer, 1, bytes, stdout) == bytes ? 0 : -1;
}

static int graph_allowed(uint64_t value, const uint64_t *graphs, uint64_t count) {
    uint64_t low = 0;
    uint64_t high = count;
    while (low < high) {
        uint64_t middle = low + (high - low) / 2;
        if (graphs[middle] < value) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    return low < count && graphs[low] == value;
}

int main(void) {
    static const unsigned char input_magic[8] = {'N','G','K','G','O','M','P','1'};
    static const unsigned char output_magic[8] = {'N','G','K','G','O','U','T','1'};
    unsigned char magic[8];
    uint64_t count = 0;
    uint64_t flags = 0;
    uint64_t values[4] = {0, 0, 0, 0};
    uint64_t graph_count = 0;
    if (read_exact(magic, sizeof magic) || memcmp(magic, input_magic, sizeof magic) ||
        read_exact(&count, sizeof count) || count == 0 || count > MAX_ROWS ||
        read_exact(&flags, sizeof flags) || (flags & ~UINT64_C(31)) != 0 ||
        read_exact(values, sizeof values) ||
        read_exact(&graph_count, sizeof graph_count) ||
        graph_count == 0 || graph_count > MAX_ALLOWED_GRAPHS) {
        return 65;
    }
    uint64_t *graphs = calloc((size_t)graph_count, sizeof *graphs);
    struct row *rows = calloc((size_t)count, sizeof *rows);
    uint8_t *matches = calloc((size_t)count, sizeof *matches);
    if (!graphs || !rows || !matches || read_exact(graphs, (size_t)graph_count * sizeof *graphs)) {
        free(graphs); free(rows); free(matches);
        return 71;
    }
    for (uint64_t index = 1; index < graph_count; ++index) {
        if (graphs[index - 1] >= graphs[index]) {
            free(graphs); free(rows); free(matches);
            return 65;
        }
    }
    for (uint64_t index = 0; index < count; ++index) {
        if (read_exact(&rows[index].subject, sizeof(uint64_t)) ||
            read_exact(&rows[index].predicate, sizeof(uint64_t)) ||
            read_exact(&rows[index].object, sizeof(uint64_t)) ||
            read_exact(&rows[index].graph, sizeof(uint64_t)) ||
            read_exact(&rows[index].queryable, sizeof(uint8_t)) ||
            rows[index].queryable > 1) {
            free(graphs); free(rows); free(matches);
            return 65;
        }
    }

    omp_set_dynamic(0);
    omp_set_max_active_levels(1);
    #pragma omp parallel for schedule(static)
    for (uint64_t index = 0; index < count; ++index) {
        const struct row *row = &rows[index];
        matches[index] = (uint8_t)(graph_allowed(row->graph, graphs, graph_count) &&
            (!(flags & UINT64_C(1)) || row->subject == values[0]) &&
            (!(flags & UINT64_C(2)) || row->predicate == values[1]) &&
            (!(flags & UINT64_C(4)) || row->object == values[2]) &&
            (!(flags & UINT64_C(8)) || row->graph == values[3]) &&
            (!(flags & UINT64_C(16)) || row->queryable));
    }
    int failed = write_exact(output_magic, sizeof output_magic) ||
                 write_exact(&count, sizeof count) ||
                 write_exact(matches, (size_t)count) || fflush(stdout);
    free(graphs); free(rows); free(matches);
    return failed ? 74 : 0;
}

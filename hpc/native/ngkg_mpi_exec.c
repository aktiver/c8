#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <mpi.h>
#include <spawn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;

static int set_rank_environment(int rank, int size, int local_rank) {
    char rank_text[32];
    char size_text[32];
    char local_text[32];
    if (snprintf(rank_text, sizeof rank_text, "%d", rank) < 1 ||
        snprintf(size_text, sizeof size_text, "%d", size) < 1 ||
        snprintf(local_text, sizeof local_text, "%d", local_rank) < 1) {
        return -1;
    }
    return setenv("NGKG_MPI_RANK", rank_text, 1) ||
           setenv("NGKG_MPI_WORLD_SIZE", size_text, 1) ||
           setenv("NGKG_MPI_LOCAL_RANK", local_text, 1);
}

int main(int argc, char **argv) {
    int provided = 0;
    if (argc < 2) {
        fputs("usage: ngkg-mpi-exec COMMAND [ARG ...]\n", stderr);
        return 64;
    }
    int init_status = MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
    if (init_status != MPI_SUCCESS) {
        fputs("MPI runtime did not provide MPI_THREAD_FUNNELED\n", stderr);
        return 70;
    }
    if (provided < MPI_THREAD_FUNNELED) {
        fputs("MPI runtime did not provide MPI_THREAD_FUNNELED\n", stderr);
        MPI_Finalize();
        return 70;
    }
    int rank = -1;
    int size = 0;
    int local_rank = -1;
    MPI_Comm local_comm = MPI_COMM_NULL;
    int failure = MPI_Comm_rank(MPI_COMM_WORLD, &rank) != MPI_SUCCESS ||
                  MPI_Comm_size(MPI_COMM_WORLD, &size) != MPI_SUCCESS ||
                  MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, rank,
                                      MPI_INFO_NULL, &local_comm) != MPI_SUCCESS ||
                  MPI_Comm_rank(local_comm, &local_rank) != MPI_SUCCESS ||
                  size < 2 || rank < 0 || rank >= size || local_rank < 0;
    if (!failure) {
        failure = set_rank_environment(rank, size, local_rank) != 0;
    }

    int local_status = 70;
    if (!failure) {
        pid_t child = -1;
        int spawn_status = posix_spawnp(&child, argv[1], NULL, NULL, &argv[1], environ);
        if (spawn_status == 0) {
            int wait_status = 0;
            pid_t waited;
            do {
                waited = waitpid(child, &wait_status, 0);
            } while (waited < 0 && errno == EINTR);
            if (waited == child && WIFEXITED(wait_status)) {
                local_status = WEXITSTATUS(wait_status);
            } else {
                local_status = 70;
            }
        }
    }

    int global_status = 70;
    if (MPI_Allreduce(&local_status, &global_status, 1, MPI_INT, MPI_MAX,
                      MPI_COMM_WORLD) != MPI_SUCCESS) {
        global_status = 70;
    }
    if (MPI_Barrier(MPI_COMM_WORLD) != MPI_SUCCESS) {
        global_status = 70;
    }
    if (local_comm != MPI_COMM_NULL) {
        MPI_Comm_free(&local_comm);
    }
    MPI_Finalize();
    return global_status;
}

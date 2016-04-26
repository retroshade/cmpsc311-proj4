/*
 * Implementation file for simple MapReduce framework.  Fill in the functions
 * and definitions in this file to complete the assignment.
 *
 * Place all of your implementation code in this file.  You are encouraged to
 * create helper functions wherever it is necessary or will make your code
 * clearer.  For these functions, you should follow the practice of declaring
 * them "static" and not including them in the header file (which should only be
 * used for the *public-facing* API.
 */


/* Header includes */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <pthread.h>
#include <errno.h>
#include <assert.h>

#include "mapreduce.h"


/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024


/* Return value from workers */
struct return_val {
	int rv;
};

/* Argument for map workers */
struct map_arg {
	struct map_reduce *mr;
	int id;
	int infd;
};

/* Code for map workers */
static void *
map_main(void *arg)
{
	struct map_arg *ma = arg;

	
        //if (connect(ma->mr->clifd[ma->id],(struct sockaddr *)&ma->mr->server_addr, sizeof(ma->mr->server_addr)) < 0) 
	//perror("ERROR connecting");
	

	// Run map callback
	int maprv = ma->mr->map(ma->mr, ma->infd, ma->id, ma->mr->nmaps);

	// Close input file
	close(ma->infd);

	// Argument was malloc'd by parent thread; free it here
	free(arg);

	// Allocate return structure (to be freed by joiner)
	struct return_val *rvs = malloc(sizeof(*rvs));
	if (!rvs) {
		perror("malloc");
		return NULL;
	}
	rvs->rv = maprv;

	return rvs;
}

struct reduce_arg {
	struct map_reduce *mr;
	int outfd;
};

/* Code for reduce worker */
static void * reduce_main(void *arg)
{
	// accept() should happen in this function
  
  
	// wait for a specific number of connections then do something with all of them
	// keep track of the server fd's returned by accept

	struct reduce_arg *ra = arg;

	// Run reduce callback
	int rv = ra->mr->reduce(ra->mr, ra->outfd, ra->mr->nmaps);

	// Close output file
	close(ra->outfd);

	// Argument was malloc'd by parent, free here
	free(arg);

	// Allocate return structure (to be freed by joiner)
	struct return_val *rvs = malloc(sizeof(*rvs));
	if (!rvs) {
		perror("malloc");
		return NULL;
	}
	rvs->rv = rv;

	return rvs;
}

/* Allocates and initializes an instance of the MapReduce framework */
struct map_reduce *
mr_create(map_fn map, reduce_fn reduce, int nmaps)
{
	// Allocate space for the instance struct itself
	struct map_reduce *mr = malloc(sizeof(*mr));
	if (!mr) {
		perror("malloc(mr)");
		return NULL;
	}

	// Allocate an array to keep track of the map pthreads
	mr->map_threads = malloc(nmaps * sizeof(mr->map_threads[0]));
	if (!mr->map_threads) {
		perror("malloc(map_threads)");
		goto err_free;
	}

	// Allocate array to keep track of mapper file descriptors
	mr->clifd = malloc((nmaps * sizeof(int)));
	if (!mr->clifd) {
		perror("malloc(clifd)");
		goto err_clifd;
	}

	// Fill in remaining fields
	mr->nmaps = nmaps;
	mr->map = map;
	//if (!(mr->map = map)) {
	//printf("error assigning map\n");
	//}
	mr->reduce = reduce;


	return mr;


	// Error cleanup
	  // err_freebufs:
	//free(mr->buffers);
           err_freemap:
	free(mr->map_threads);
           err_free:
	free(mr);
           err_clifd:
	free(mr->clifd);
	return NULL;
}

/* Destroys and cleans up an existing instance of the MapReduce framework */
void
mr_destroy(struct map_reduce *mr)
{
	//free(mr->buffers);
	free(mr->map_threads);
	free(mr->clifd);
	//close(mr->sock_s);
	//close(mr->sock_c);
	free(mr);
}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *path, const char *ip, uint16_t port)
{
	int fd = 0;

	// create sockets for map threads
	int r;
	for (r = 0; r < mr->nmaps; r++) {
	    mr->clifd[r] = socket(AF_INET, SOCK_STREAM, 0);
	    if (mr->clifd[r] <= 0) {
		perror("error creating client socket");
		// free/close something??
	    }
	}

	// Create the map threads
	int i;
	for (i = 0; i < mr->nmaps; i++) {
		// Open input file (map thread will close)
		fd = open(path, O_RDONLY);
		if (fd < 0) {
			perror("open");
			return 1;
		}

		// Allocate/set up argument for thread (map thread will free)
		struct map_arg *ma = malloc(sizeof(*ma));
		if (!ma) {
			perror("malloc(map_arg)");
			close(fd);
			return 1;
		}
		ma->mr = mr;
		ma->id = i;
		ma->infd = fd;

		

		// Create and launch map thread
		if (pthread_create(&mr->map_threads[i], NULL, map_main, ma)) {
			perror("pthread_create(map)");
			close(fd);
			free(ma);
			return 1;
		}

		// If this succeeds, the thread is now responsible to clean up
		// the resources we passed to it.
	}

	// Set up reduce args
	struct reduce_arg *ra = malloc(sizeof(*ra));
	if (!ra) {
		perror("malloc(reduce_arg)");
		close(fd);
		return 1;
	}
	ra->mr = mr;
	ra->outfd = fd;

	// create socket for reduce thread
	mr->serverfd = socket(AF_INET, SOCK_STREAM, 0);
	if (mr->serverfd <= 0) {
		perror("Error creating server socket");
	}

	// use bind() to tell the server socket its information
	mr->server_addr.sin_family = AF_INET;
	mr->server_addr.sin_addr.s_addr = INADDR_ANY;
	mr->server_addr.sin_port = htons(port);

	if (bind(mr->serverfd, (struct sockaddr*)&mr->server_addr, sizeof(mr->server_addr)) < 0)
	   perror("Error upon binding");

	// listen
	listen(mr->serverfd, 64);

	// Create the reduce thread
	if (pthread_create(&mr->reduce_thread, NULL, reduce_main, ra)) {
		perror("pthread_create(reduce)");
		close(fd);
		free(ra);
		return 1;
	}

	// what is argv[1] for server = gethostbyname?
	//mr->server = gethostbyname("localhost");
	//mr->server_addr.sin_family = AF_INET;
	//mr->client_addr.sin_family = AF_INET;
	//mr->server_addr.sin_port = htons(port);
	//if (bind(mr->sock_s, (struct sockaddr*)&mr->server_addr, sizeof(mr->server_addr)) < 0)
	 // perror("Error on binding\n");

	mr->clilen = sizeof(mr->client_addr);

	//if (connect(mr->sock_s, (struct sockaddr*)&mr->server_addr, sizeof(mr->server_addr)) < 0)
	  //perror("Error connecting to server\n");

	//mr->newsock_s = accept(mr->sock_s, (struct sockaddr*)&mr->client_addr, &mr->clilen);
	//if (mr->newsock_s < 0)
	  //perror("Error accepting\n");

	// If this succeeds, the thread is now responsible to clean up the
	// resources we passed to it.

	return 0;
}

/* Blocks until the entire MapReduce operation is complete */
int
mr_finish(struct map_reduce *mr)
{
	int rv = 0;

	// Join with each of the map threads
	int i;
	void *void_rv;
	struct return_val *thread_rv;
	for (i = 0; i < mr->nmaps; i++) {
		if (pthread_join(mr->map_threads[i], &void_rv)) {
			perror("pthread_join(map)");
			return 1;
		}
		thread_rv = void_rv;
		if (!thread_rv || thread_rv->rv != 0)
			rv = 1;
		// free(NULL) is safe by POSIX
		free(thread_rv);
	}

	// Join with the reduce thread
	if (pthread_join(mr->reduce_thread, &void_rv)) {
		perror("pthread_join(reduce)");
		return 1;
	}
	thread_rv = void_rv;
	if (!thread_rv || thread_rv->rv != 0)
		rv = 1;
	// free(NULL) is safe by POSIX
	free(thread_rv);

	return rv;
}

/* Called by the Map function each time it produces a key-value pair */
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv)
{
	int rv;
	//struct pcbuf *pc = &mr->buffers[id];

	// Calculate total needed size
	uint32_t totalsz = sizeof(kv->keysz) + kv->keysz +
		sizeof(kv->valuesz) + kv->valuesz;

	// Make sure it will actually fit, ever
	//if (totalsz > pc->capacity) {
		// The error "File too large" seems appropriate
	//	errno = EFBIG;
	//	return -1;
	//}

	// Wait for enough room
	//while (pc->used + totalsz > pc->capacity) {
	//	rv = pthread_cond_wait(&pc->cons, &pc->lock);
		// Error here indicates a bug in our code
	//	assert(rv == 0);
	//}

	// XXX For now, assume shift-style buffer
	//assert(pc->pos == 0);

	return 1;
}

/* Called by the Reduce function to consume a key-value pair */
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv)
{
	int rv;
	//struct pcbuf *pc = &mr->buffers[id];

	// XXX For now, assume shift-style buffer
	//assert(pc->pos == 0);

	return 1;
}


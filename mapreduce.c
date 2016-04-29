
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
#include <arpa/inet.h>

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

        if (connect(ma->mr->clifd[ma->id],(struct sockaddr *)&ma->mr->server_addr, sizeof(ma->mr->server_addr)) < 0) 
	perror("ERROR connecting");
	
	int maprv = 0;
	
	if (ma->mr->map == NULL) {
	    maprv = -1;    
	}	
	
	// Run map callback
	if (maprv >= 0)
	   maprv = ma->mr->map(ma->mr, ma->infd, ma->id, ma->mr->nmaps);

	// Close input file
	close(ma->infd);
	ma->infd = -1;

	// Argument was malloc'd by parent thread; free it here
	free(arg);

	// Allocate return structure (to be freed by joiner)
	struct return_val *rvs = malloc(sizeof(*rvs));
	if (!rvs) {
		perror("malloc");
		return NULL;
	}
	rvs->rv = maprv;

	if (rvs == NULL) {
		free(rvs);
		return NULL;
	}

	return rvs;
}

struct reduce_arg {
	struct map_reduce *mr;
	int outfd;
};

/* Code for reduce worker */
static void * reduce_main(void *arg)
{
  
	// wait for a specific number of connections then do something with all of them
	// keep track of the server fd's returned by accept

	struct reduce_arg *ra = arg;

	int rv = 0;
	
	if (ra->mr->reduce == NULL)
	   rv = -1;



	int i;
	for (i = 0; i < ra->mr->nmaps; i++) {
		ra->mr->acceptedserverfd[i] = accept(ra->mr->serverfd, (struct sockaddr *)&ra->mr->client_addr, &ra->mr->clilen);
       	if (ra->mr->acceptedserverfd[i] < 0)
	  perror("Failed to accept");
	
	}
	// Run reduce callback
	if (rv >= 0)
	    rv = ra->mr->reduce(ra->mr, ra->outfd, ra->mr->nmaps);

	// Close output file
	close(ra->outfd);
	ra->outfd = -1;

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

	mr->acceptedserverfd = malloc((nmaps * sizeof(int)));
	if (!mr->acceptedserverfd) {
		perror("malloc(acceptedserverfd)");
		goto err_acceptedserverfd;
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
	   err_acceptedserverfd:
	free(mr->acceptedserverfd);
	return NULL;
}

/* Destroys and cleans up an existing instance of the MapReduce framework */
void
mr_destroy(struct map_reduce *mr)
{
  free(mr->map_threads);

  free(mr->clifd);
  free(mr->acceptedserverfd);
  free(mr);
}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *path, const char *ip, uint16_t port)
{
  int fd = 0;
  
  // give addresses values
  memset(&mr->server_addr, 0, sizeof(mr->server_addr));
  memset(&mr->client_addr, 0, sizeof(mr->client_addr));
  mr->server_addr.sin_family = AF_INET;
  mr->client_addr.sin_family = AF_INET;
  inet_pton(AF_INET, ip, &mr->server_addr.sin_addr);
  inet_pton(AF_INET, ip, &mr->client_addr.sin_addr);
  mr->server_addr.sin_port = htons(port);
  mr->client_addr.sin_port = htons(port);


  if (mr->map != NULL) {
    
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
  }

  else if (mr->reduce != NULL) {
    // Set up reduce args
    struct reduce_arg *ra = malloc(sizeof(*ra));
    if (!ra) {
      perror("malloc(reduce_arg)");
      close(fd);
      return 1;
    }
    
    fd = -1;
    fd = open(path, O_WRONLY | O_TRUNC);
    if (fd < 0) {
      // make a new file
      FILE *fi;
      fi = fopen(path, "w");
      fd = fileno(fi);
    }
    ra->outfd = fd;
    ra->mr = mr;
   mr->clilen = sizeof(mr->client_addr);

    // create socket for reduce thread
    mr->serverfd = socket(AF_INET, SOCK_STREAM, 0);
    if (mr->serverfd <= 0) {
      perror("Error creating server socket");
    }

    	if (bind(ra->mr->serverfd, (struct sockaddr*)&ra->mr->server_addr, sizeof(ra->mr->server_addr)) < 0)
	   perror("Error upon binding");

	listen(ra->mr->serverfd, 64);

    // Create the reduce thread
    if (pthread_create(&mr->reduce_thread, NULL, reduce_main, ra)) {
      perror("pthread_create(reduce)");
      close(fd);
      free(ra);
      return 1;
    }
  }

  else
    return -1;
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
  if (mr->map != NULL) {
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
  }

  // Join with the reduce thread
  if (mr->reduce != NULL) {
    if (pthread_join(mr->reduce_thread, &void_rv)) {
      perror("pthread_join(reduce)");
      return 1;
    }
	
    thread_rv = void_rv;
    if (!thread_rv || thread_rv->rv != 0)
      rv = 1;
    // free(NULL) is safe by POSIX
    if (thread_rv != NULL)
      free(thread_rv);
  }


  if (mr->map == NULL) {
    close(mr->serverfd);
    int j;
    for (j = 0; j < mr->nmaps; j++) {
      close(mr->acceptedserverfd[j]);
    }
  }
  if (mr->reduce == NULL) {
    int j;
    for (j = 0; j < mr->nmaps; j++) {
      close(mr->clifd[j]);
    }
}
  return rv;
}

/* Called by the Map function each time it produces a key-value pair */
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv)
{
  // returns 1 if successful and -1 on error
  // copy kv pair into the buffer

  int increment = 0;

  if (kv != NULL) {
    memcpy(&mr->buf[increment], &kv->keysz, (int)sizeof(&kv->keysz));
    increment += (int)sizeof(&kv->keysz);
    memcpy(&mr->buf[increment], &kv->valuesz, (int)sizeof(&kv->valuesz));
    increment += (int)sizeof(&kv->valuesz);
    memcpy(&mr->buf[increment], kv->key, kv->keysz);
    increment += kv->keysz;
    memcpy(&mr->buf[increment], kv->value, kv->valuesz);
    increment += kv->valuesz;


    send(mr->clifd[id], mr->buf, sizeof(mr->buf), 0);
    return 1;
   }
  else
    return -1;
  return 0;
}

/* Called by the Reduce function to consume a key-value pair */
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv)
{
  // returns -1 upon error
  // returns 0 if  nothing consumed
  // returned 1 upon success
  int rv = -1;
  int increment = 0;

  rv = recv(mr->acceptedserverfd[id], mr->buf, sizeof(mr->buf), 0);
  if (rv > 0) {
    // it read something from the buffer- kv pair exists
    memcpy(&kv->keysz, &mr->buf[increment], (int)sizeof(&kv->keysz));
    increment += (int)sizeof(&kv->keysz);
    memcpy(&kv->valuesz, &mr->buf[increment], (int)sizeof(&kv->valuesz));
    increment += (int)sizeof(&kv->valuesz);
    memcpy(kv->key, &mr->buf[increment], kv->keysz);
    increment += kv->keysz;
    memcpy(kv->value, &mr->buf[increment], kv->valuesz);
    increment += kv->valuesz;
    return 1;
  }
  else if (rv < 0) {
    return -1;
  }
  else
    return 0;
}

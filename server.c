/* Server program for key-value store. */
#include "kv.h"
#include "parser.h"

#include <unistd.h>
#include <strings.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <poll.h>

#define  BUFFER_SIZE 255
#define NTHREADS 4
#define BACKLOG 10

/* Add anything you want here. */
pthread_mutex_t process;
pthread_cond_t wait;
int freeWorker = NTHREADS;
int workerBusy[NTHREADS];
int connSock[NTHREADS];

/* A worker thread. You should write the code of this function. */
void* worker(void* p) {
	int sockControl;
	int workerID = *(int *)p;
	int run = 1;
	char recvbuf[BUFFER_SIZE];
  	char cmdBuffer[BUFFER_SIZE];
  	char *key, *value;
  	enum DATA_CMD cmd;
  	while(run == 1)
  	{

  		if(pthread_mutex_lock(&process)!=0){
    		perror("pthread_mutex_lock() error");
    		exit(1);
  		}
  		while(workerBusy[workerID] == 0){
    		if(pthread_cond_wait(&wait, &process)!=0){
      			perror("pthread_cond_wait() error.");
     			 exit(1);
    		}
  		}
  		if(pthread_mutex_unlock(&process)!=0){
    		perror("pthread_mutex_unlock() error");
    		exit(1);
  		}
    	strcpy(cmdBuffer, "Welcome to the KV store.\n");
   	 	if(write(connSock[workerID],cmdBuffer,strlen(cmdBuffer))<0)
    	{
        	perror("write function error occurred! \n");
        	exit(1);
    	}

    	while(workerBusy[workerID]>0)
    	{
    		memset(recvbuf, 0, sizeof(recvbuf));
    		if(read(connSock[workerID], recvbuf, sizeof(recvbuf))<0){
        		perror("read function error occurred!\n");
        		exit(1);
    		}
   			parse_d(recvbuf,&cmd, &key, &value);
     /*put KEY VALUE*/

    		if(pthread_mutex_lock(&process)!=0){
      			perror("pthread_mutex_lock() error");
      			exit(1);
    		}
    		bzero(cmdBuffer,sizeof(cmdBuffer));

    		if(cmd == D_PUT){
      			char* kvValue = malloc(strlen(value)+1);
      		if(kvValue == NULL){
        		return NULL;
      		}
      		strncpy(kvValue, value, strlen(value)+1);

      		sockControl = createItem(key,kvValue);

      		if(sockControl !=-1){
        		strcpy(cmdBuffer, "Stored key successfully.\n");

      		}else if(itemExists(key) == 1){
        		sockControl = updateItem(key, value);
        		if(sockControl!=-1){
          			strcpy(cmdBuffer, "Updated key successfully.\n");
        		}else{
          			strcpy(cmdBuffer, "Error updating key.\n");
        		}
      		}else{
        		strcpy(cmdBuffer, "Error storing key.\n");
      		}
    		}
    		if(cmd == D_GET){
    			if(itemExists(key) == 1){
      				strcpy(cmdBuffer, findValue(key));
      				strcat(cmdBuffer, "\n");
    			}else{
      				strcpy(cmdBuffer, "No such key.\n");
    			}
  			}


			if(cmd == D_COUNT){
      			sockControl = countItems();
      			sprintf(cmdBuffer,"%d\n", sockControl);
  			}

  			if(cmd == D_DELETE){
    			sockControl = deleteItem(key, 1);
    			if(sockControl == 0){
      				strcpy(cmdBuffer, "delete key successfully.\n");
    			}else{
      				strcpy(cmdBuffer, "Error for delete key.\n");
   				}
  			}
  			if(cmd == D_EXISTS){
    			sockControl = itemExists(key);
    			if(sockControl == 1){
      				strcpy(cmdBuffer, "1\n");
    			}else{
      				strcpy(cmdBuffer, "0\n");
    			}
  			}
  			if(cmd == D_END){
    			strcpy(cmdBuffer, "goodbye\n");
    			printf("goodbye\n");
    			workerBusy[workerID]--;
  			}
			if(cmd == D_ERR_OL){
			    strcpy(cmdBuffer, "Error: the line is too long.\n");
			  }
			  if(cmd == D_ERR_INVALID){
			    strcpy(cmdBuffer, "Error: invalid command.\n");
			  }
			  if(cmd == D_ERR_SHORT){
			    strcpy(cmdBuffer, "Error: too few parameters.\n");
			  }
			  if(cmd == D_ERR_LONG){
			    strcpy(cmdBuffer, "Error: too many parameters.\n");
			  }
			  if(write(connSock[workerID], &cmdBuffer, sizeof(cmdBuffer))<0)
			  {
			    perror("write function error occurred\n");
			    exit(1);
			  }
			  if(pthread_mutex_unlock(&process)!=0){
    			perror("pthread_mutex_unlock() error");
    			exit(1);
  			}
    	}
		if(pthread_mutex_lock(&process)!=0){
		    perror("pthread_mutex_lock() error");
		    exit(1);
		 }
		 workerBusy[workerID]=0;
		 freeWorker++;
		 if(pthread_mutex_unlock(&process)!=0){
		     perror("pthread_mutex_unlock() error");
		     exit(1);
		 }
		  close(connSock[workerID]);
		 if(pthread_cond_broadcast(&wait)!=0){
		      perror("pthread_cond_broadcast() error!");
		      exit(1);
		 }
  	}
}

/* You may add code to the main() function. */
int main(int argc, char** argv) {
    int cport, dport; /* control and data ports. */
    int i, sockSer1, sockSer2;
    struct pollfd pfd[2];

	if (argc < 3) {
        printf("Usage: %s data-port control-port\n", argv[0]);
        exit(1);
	} else {
        cport = atoi(argv[2]);
        dport = atoi(argv[1]);
	}
  pthread_t workerThreads[NTHREADS];
  if(pthread_mutex_init(&process, NULL)!=0){
    perror("pthread_mutex_init() error");
    exit(1);
  }
  if(pthread_cond_init(&wait, NULL)!=0){
    perror("pthread_cond_init() error");
    exit(1);
  }
    sockSer1 = socket(AF_INET, SOCK_STREAM, 0);
    if(sockSer1 == -1)
    {
        perror("socket");
        exit(1);
    }
    sockSer2 = socket(AF_INET, SOCK_STREAM, 0);
    if(sockSer2 == -1)
    {
    	perror("socket2");
    	exit(1);
    }

    struct sockaddr_in sa, a;
    socklen_t len = sizeof(sa);
    memset(&sa, 0, len);
    sa.sin_family = AF_INET;
    sa.sin_port = htons(cport); //control port
    sa.sin_addr.s_addr = htonl(INADDR_ANY);

    int res = bind(sockSer1, (struct sockaddr*)&sa, len);
    if(res == -1){
        perror("bind");
        exit(1);
    }
    listen(sockSer1, BACKLOG);

    socklen_t len2 = sizeof(a);
    memset(&a, 0, len2);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr  = htonl(INADDR_ANY);
    a.sin_port = htons(dport); //data port
   	int res2 = bind(sockSer2, (struct sockaddr*)&a, len2);
   	if(res2 == -1){
   		perror("bind2");
   		exit(1);
   	}
   	listen(sockSer2, BACKLOG);

    pfd[0].fd = sockSer2;
    pfd[0].events = POLLIN;

    pfd[1].fd = sockSer1;
    pfd[1].events = POLLIN;

    int run = 1;
    int timeout = -1;
    int workerID[NTHREADS];
    int n;
    for(i = 0; i<NTHREADS; i++){
      workerID[i] = i;
      pthread_create(&workerThreads[i], NULL, worker, &workerID[i]);
    }
    printf("Server started.\n");
    while(run)
    {
        int sockConn, sockConn2;
        char recvbuf[BUFFER_SIZE];
        char sendbuf[BUFFER_SIZE];
        char *writeBuffer;

        int rv = poll(pfd, 2, timeout);
        if(rv == -1){
            perror("polling error!");
            exit(1);
        }else if(rv == 0){
            printf("non-blocking, always return immediately!\n");
        }else{
            if(pfd[0].revents & POLLIN)
            {
              while(freeWorker<1){
                  if(pthread_cond_wait(&wait, &process)!=0)
                  {
                      perror("pthread_cond_wait() error");
                      exit(1);
                   }
                }
                 sockConn2 = accept(sockSer2, (struct sockaddr*)&a, &len2);
                 if(sockConn2 == -1)
                 {
                    perror("accept2");
                    exit(1);
                }else{
                  printf("Client[%d] data-port Connect Server OK.\n",a.sin_port);
                }
                    for(int id=0; id<NTHREADS; id++){
                      printf("%d\n",id);
                      if(workerBusy[id] == 0){
                        connSock[id] = sockConn2;
                        workerBusy[id] = 1;
                        freeWorker--;
                        break;
                      }
                  }
                  if(pthread_cond_broadcast(&wait)!=0)
                  {
                    perror("pthread_cond_broadcast() error!");
                    exit(1);
                  }
            }
            if(pfd[1].revents & POLLIN)
            {
                sockConn = accept(sockSer1,(struct sockaddr*)&sa, &len);

                if(sockConn == -1){
                     perror("accept");
                    exit(1);
                }
                else{
                    printf("Client[%d] control-port Connect Server OK.\n",a.sin_port);
                }

                  if(read(sockConn, recvbuf, sizeof(recvbuf))<0){
                    perror("read function error occurred!\n");
                    exit(1);
                  }
                    enum CONTROL_CMD cmd;
                    cmd = parse_c(recvbuf);
                    
                    if(cmd == C_COUNT){
                        //countItems();
                       sprintf(sendbuf, "%d\n", countItems());
                       write(sockConn, sendbuf, strlen(sendbuf));
                       close(sockConn);
                    }
                    if(cmd == C_SHUTDOWN){
                      //printf("Shutting down\n");
                      while(freeWorker<NTHREADS){
                        writeBuffer = "Shutting down.\n";
                        write(sockConn, writeBuffer, strlen(writeBuffer));
                        close(sockConn);
                      }
                     // printf("Server is shutdown\n");
                      close(sockConn);
                      close(sockConn2);
                      printf("Server is shutdown\n");
                      return 0;
                    }else if(cmd == C_ERROR){
                      sprintf(sendbuf,"%d", "error\n");
                      write(sockConn, sendbuf,strlen(sendbuf));
                      close(sockConn);
                    }
            }
        }
    }
    close(sockSer1);
    close(sockSer2);
    for(i = 0; i<NTHREADS; i++)
    {
      pthread_join(workerThreads[i], NULL);
    }
    return 0;
}

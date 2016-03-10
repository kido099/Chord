
#include <stdio.h>
#include <stdlib.h>
#include "csapp.h"
#include "sha1.h"
#include <pthread.h>
#include <math.h>
#include <unistd.h>
#include <string.h>

/* defines */
#define KEY_BITS 32
//#define MAXLINE 128

pthread_mutex_t mutex;



/* Main function for the program */
int main(int argc, char *argv[]) {
    /* variable declarations */
    char *port_to_connect, *ip_to_connect;
    int tries,serverfd;
    char *cmd, buf1[MAXLINE];
    
    if(argc == 4) {
        port_to_connect = argv[2];
        ip_to_connect = argv[3];
        
        tries = 3;
        while((serverfd = open_clientfd(ip_to_connect,atoi(port_to_connect)))<0) {
            tries--;
            if(tries < 0) printf("Failed to open connection to the node already existed!\n");
        }
        cmd = strdup("query");
        sprintf(buf1,"%s\r\n", cmd);
        Rio_writen(serverfd,buf1,strlen(buf1));

        
    }
}
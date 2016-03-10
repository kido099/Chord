#include <stdio.h>
#include <stdlib.h>
#include "csapp.h"
#include "sha1.h"
#include <pthread.h>
#include <math.h>
#include <unistd.h>
#include <string.h>
//#include "query.c"

/* defines */
#define KEY_BITS 32
//#define MAXLINE 128

pthread_mutex_t mutex;

/* variable declarations */
/* Finger */
typedef struct _Finger {
    unsigned start;
    struct _Node *node;
} Finger;
/* FingerTable */
typedef struct _FingerTable {
    struct _Finger *fingers;
    int length;
} FingerTable;
/* node */
typedef struct _Node {
    unsigned id;
    char *ipAddr;
    char *port;
    int *flag;
    /* pre */
    unsigned predecessor_id;
    char *predecessor_ip;
    char *predecessor_port;
    /* suc */
    unsigned successor_id;
    char *successor_ip;
    char *successor_port;
    /* pre pre */
    unsigned  pre_predecessor_id;
    char *pre_predecessor_ip;
    char *pre_predecessor_port;
    /* finger table */
    struct _FingerTable finger_table;
} Node;

Node *node = NULL;
FingerTable *finger_table = NULL;

/* function declarations */
unsigned hash(char *origin);
Finger* finger_init(Node *node, unsigned start);
FingerTable *finger_table_init(Node *node);
void node_create(char *port_to_join, char *ip_in_ring, char *port_in_ring);
void notify_successor(Node *node,unsigned successor_id,char *successor_ip,char *successor_port);
void *send_stabilize(void *args);
void *listen_stabilize(void *args);
void *fix_fingers(void *args);
void *listen_fix(void *args);
void *send_heart_beating(void *args);
void *listen_heart_beating(void *args);
void node_find_successor(Node *node, unsigned target_id, char *target_ip, char *target_port);
void node_find_predecessor(Node *node, unsigned target_id, char *target_ip, char *target_port);
void node_closest_preceding_node(Node *node, unsigned target_id, char *target_ip, char *target_port);
int key_in_range(unsigned target_id, unsigned node_id, unsigned node_successor_id);
unsigned atou(char *id);
void start_find_successor(Node *node, int num, unsigned target_id, char *target_ip, char *target_port);
void start_find_predecessor(Node *node, int num, unsigned target_id, char *target_ip, char *target_port);
void start_closest_preceding_node(Node *node, int num, unsigned target_id, char *target_ip, char *target_port);

/* function implementations */
unsigned hash(char *origin) {
    SHA1_CTX sha;
    uint8_t results[20];
    char *buf;
    int size;
    uint8_t chunks0[4];
    uint8_t chunks1[4];
    uint8_t chunks2[4];
    uint8_t chunks3[4];
    uint8_t chunks4[4];
    uint8_t ans[4];
    
    buf = strdup(origin);
    size = strlen(buf);
    SHA1Init(&sha);
    SHA1Update(&sha,(uint8_t *)buf,size);
    SHA1Final(results,&sha);
    
    strncpy(chunks0,results,4);
    strncpy(chunks1,results + 4,4);
    strncpy(chunks2,results + 4*2,4);
    strncpy(chunks3,results + 4*3,4);
    strncpy(chunks4,results + 4*4,4);
    
    for(int i = 0;i < 4;i++) {
        ans[i] = chunks0[i] ^ chunks1[i] ^ chunks2[i] ^ chunks3[i] ^ chunks4[i];
    }
    return *(unsigned *)ans;
}
Finger* finger_init(Node *node, unsigned start) {
    Finger *finger = NULL;
    
    if ((finger = malloc(sizeof(Finger))) == NULL) {
        printf("Failed to allocate memory for Finger");
    }
    
    finger->node = node;
    finger->start = start;
    
    return finger;
}
FingerTable *finger_table_init(Node *node) {
    Finger *finger = NULL;
    unsigned start;
    /* allocate finger table memory */
    finger_table = malloc(sizeof(FingerTable));
    
    finger_table->length = KEY_BITS;
    /* allocate fingers in the table */
    if ((finger_table->fingers = malloc(sizeof(Finger) * finger_table->length)) == NULL) {
        printf("Failed to allocate memory for Finger");
    }
    for (int i = 0; i < finger_table->length; i++) {
        start = node->id + pow(2, i);
        if (start > pow(2,KEY_BITS)) {
            start -= pow(2,KEY_BITS);
        }
        finger = finger_init(node, start);
        finger_table->fingers[i] = *finger;
    }
    
    return finger_table;
}
void node_create(char *port_to_join, char *ip_in_ring, char *port_in_ring) {
    int listenfd, connfd, clientfd, serverfd, clientlen, tries;
    struct sockaddr_in clientaddr;
    char *host, buf1[MAXLINE];
    char *cmd, *saveptr;
    char *target_ip, *target_port, *successor_ip, *successor_port, *predecessor_ip, *predecessor_port;
    unsigned target_id, successor_id, predecessor_id;
    Node  **args1, **args2, **args3, **args4, **args5, **args6;
    pthread_t tid;
    rio_t client;
    
    //printf("in node_create 1\n");
    
    printf("node_port: %s\n", port_to_join);
    //Node *node = NULL;
    if ((node = malloc(sizeof(Node))) == NULL) {
        printf("Failed to allocate memory for Node");
    }
    //printf("in node_create 2\n");
    node->port = port_to_join;
    node->ipAddr = "127.0.0.1";      //get the ip address
    char *origin= malloc((strlen(node->ipAddr) + strlen(node->port) + 3) * sizeof(*origin));
    sprintf(origin,"%s:%s\r\n", node->ipAddr, node->port);
    printf("origin: %s\n",origin);
    unsigned hashID = hash(origin);
    printf("in\n");
    node->id = hashID;
    node->flag = malloc(sizeof(int));
    *(node->flag) = 0;
    printf("hashid:%u\n",node->id);
    printf("flag:%d\n",*(node->flag));
    node->predecessor_id = node->id;
    node->predecessor_ip = node->ipAddr;
    node->predecessor_port = node->port;
    node->successor_id = node->id;
    node->successor_ip = node->ipAddr;
    node->successor_port = node->port;
    node->pre_predecessor_id = node->id;
    node->pre_predecessor_ip = node->ipAddr;
    node->pre_predecessor_port = node->port;
    node->finger_table = *finger_table_init(node);
    


   printf("Joining the Chord ring.\n");
   printf("You are listening on port %s.\n", node->port);
   printf("Your position is %u.\n", node->id);
    /* create the thread to handle heartbeating
    args1 = malloc(sizeof(Node *));
    args1[0] = node;
    printf("before heart\n");
    Pthread_create(&tid,NULL,send_heart_beating,(void *)args1);
    Pthread_detach(tid);
    
    printf("before listen\n");
    args2 = malloc(sizeof(Node *));
    args2[0] = node;
    Pthread_create(&tid,NULL,listen_heart_beating,(void *)args2);
    Pthread_detach(tid);
    */
    
    /* create the thread to handle the stabilize */
     args3 = malloc(sizeof(Node *));
     args3[0] = node;
     Pthread_create(&tid,NULL,send_stabilize,(void *)args3);
     Pthread_detach(tid);
     
     args4 = malloc(sizeof(Node *));
     args4[0] = node;
     Pthread_create(&tid,NULL,listen_stabilize,(void *)args4);
     Pthread_detach(tid);
    
    
    /* create the thread to handle the fix fingers */
     args5 = malloc(sizeof(Node *));
     args5[0] = node;
     Pthread_create(&tid,NULL,fix_fingers,(void *)args5);
     Pthread_detach(tid);
    
    args6 = malloc(sizeof(Node *));
    args6[0] = node;
    Pthread_create(&tid,NULL,listen_fix,(void *)args6);
    Pthread_detach(tid);
    
    
    /* if not the first node */
    if(strcmp(port_in_ring,"-1") != 0) {
        printf("in join\n");
        host = strdup(ip_in_ring);
        tries = 3;
        printf("host,port: %s %s\n",host,port_in_ring);
        while((serverfd = open_clientfd(host,atoi(port_in_ring)))<0) {
            tries--;
            printf("1\n");
            if(tries < 0) printf("Failed to open connection to the node already existed!\n");
        }
        cmd = strdup("find_successor");
        sprintf(buf1,"%s %s %s %d\r\n", cmd,node->ipAddr,node->port,node->id);
        printf("start join write\n");
        Rio_writen(serverfd,buf1,strlen(buf1));
    }
    
    /* start listening on the node->port */
    printf("start listen\n");
    listenfd = Open_listenfd(atoi(node->port));
    printf("listenfd: %d\n",listenfd);
    while(1) {
        printf("in listen cmd while\n");
        clientlen = sizeof(clientaddr);
        /* accept a new connection from a client here */
        printf("start accept new cmd\n");
        connfd = Accept(listenfd, (SA *)&clientaddr, &clientlen);//keep listening
        printf("accept new cmd success\n");
        clientfd = connfd;
        printf("listen cmd listenfd: %d\n",listenfd);
        Rio_readinitb(&client, clientfd);
        Rio_readlineb(&client, buf1, MAXLINE);
        
        /* Determine cmd type */
        cmd = strtok_r(buf1," ",&saveptr);
        printf("cmd: %s\n",cmd);
        /* a new node joins */
        if(strcmp(cmd,"find_successor") == 0) {
            target_ip = strtok_r(NULL," ",&saveptr);
            target_port = strtok_r(NULL," ",&saveptr);
            target_id = atou(strtok_r(NULL,"\r\n",&saveptr));
            printf("Receive find_successor cmd..target_ip,port,id: %s %s %d\n",target_ip,target_port,target_id);
            /* find successor */
            printf("before call find succ\n");
            node_find_successor(node,target_id,target_ip,target_port);
        }
        else if(strcmp(cmd,"Results_of_find_successor") == 0) {
            printf("into result of suc\n");
            /* cmd, successor_id, successor_origin */
            successor_ip = strtok_r(NULL," ",&saveptr);
            successor_port = strtok_r(NULL," ",&saveptr);
            // printf("22results of find succ: %s %s\n",successor_ip,successor_port);
            successor_id = atou(strtok_r(NULL,"\r\n",&saveptr));
            printf("1suc_id,ip,port: %d %s %s\n", successor_id,successor_ip,successor_port);
            /* the information above to be used in the update... */
            //char *successor_update = malloc((strlen(successor_ip) + strlen(successor_port) + 3) * sizeof(*successor_update));
            //sprintf(successor_update,"%s:%s\r\n", successor_ip, successor_port);
            node->successor_id = successor_id;
            node->successor_ip = strdup(successor_ip);
            node->successor_port = strdup(successor_port);
            printf("Your successor is node %s, port %s, position %u.\n", node->successor_ip, node->successor_port, node->successor_id);
            printf("suc_id,ip port: %d %s %s\n", node->successor_id,node->successor_ip,node->successor_port);
            /* notify successor */
            printf("before notify successor\n");
            notify_successor(node,node->successor_id,node->successor_ip,node->successor_port);
        }
        else if(strcmp(cmd,"Be_notified") == 0) {
            printf("in be notified\n");
            predecessor_id = atou(strtok_r(NULL," ",&saveptr));
            predecessor_ip = strtok_r(NULL," ",&saveptr);
            predecessor_port = strtok_r(NULL,"\r\n",&saveptr);
            //char *predecessor_update = malloc((strlen(predecessor_ip) + strlen(predecessor_port) + 3) * sizeof(*predecessor_update));
            //sprintf(predecessor_update,"%s:%s\r\n", predecessor_ip, predecessor_port);
            node->predecessor_id = predecessor_id;
            node->predecessor_ip = strdup(predecessor_ip);
            node->predecessor_port = strdup(predecessor_port);
            printf("wwwwwwwAfter being notified...pre_id,pre_ip,pre_port: %u %s %s\n",node->predecessor_id,node->predecessor_ip,node->predecessor_port);
            printf("Your predecessor is node %s, port %s, position %u.\n", node->predecessor_ip, node->predecessor_port, node->predecessor_id);
            
        }
        else if(strcmp(cmd,"query") == 0) {
            printf("In query\n");
            /*unsigned search_id = atou(strtok_r(NULL," ",&saveptr));
            char *search_ip = strtok_r(NULL," ",&saveptr);
            char *search_port = strtok_r(NULL,"\r\n",&saveptr);*/
            printf("query %s %s\n",node->ipAddr,node->port);
            printf("Connection to node %s, port %s, position %u\n",node->ipAddr,node->port,node->id);
            printf("Please enter your search key(or type 'quit' to leave\n)");
            FILE *stdin;
            char name[64];
            fgets(name,64,stdin);
            unsigned ID = hash(name);
            printf("Hash value is %u",ID);
            node_find_successor(node,ID,target_ip,target_port);
            
        }
    }
}
/* implement own atou */
unsigned atou(char *id) {
    int i=0;
    unsigned result=0;
    //char *p=(id);
    while(id[i]!='\0'){
        result*=10;
        result+=id[i]-'0';
        i++;
    }
    return result;
}
/* notify */
void notify_successor(Node *node,unsigned successor_id,char *successor_ip,char *successor_port) {
    int tries,serverfd;
    char buf1[MAXLINE];
    char *cmd;
    
    printf("in notify function\n");
    tries = 3;
    while((serverfd = open_clientfd(successor_ip,atoi(successor_port)))<0) {
        tries--;
        if(tries < 0) printf("Failed to open connection to the node already existed!\n");
    }
    cmd = strdup("Be_notified");
    sprintf(buf1, "%s %d %s %s\r\n", cmd, node->id, node->ipAddr, node->port);
    printf("notify start write\n");
    Rio_writen(serverfd,buf1,strlen(buf1));
}
/* send stabilize */
void *send_stabilize(void *args) {
    int tries, serverfd;
    char buf1[MAXLINE], *cmd;
    Node *node;
    //unsigned node_id;
    
    printf("In send stablize\n");
    node = ((Node **)args)[0];
    
    /*
    successor_id = node->successor_id;
    //successor_origin = strdup(node->successor_origin);
    successor_ip = strdup(node->successor_ip);
    successor_port = strdup(node->successor_port);
    printf("su_port: %s\n",successor_port);///something wrong su_port: (null)no-por: 8001
    node_id = node->id;
    node_ip = strdup(node->ipAddr);
    node_port = strdup(node->port);
    */
    while(1) {
        
        //printf("In send stablize while\n");
        //if(node->successor_id != node->id) {
          tries = 3;
          while((serverfd = open_clientfd(node->successor_ip,atoi(node->successor_port)+5000)) < 0) {
              tries--;
              if(tries < 0) printf("Failed to open connection to the node already existed!\n");
          }
          printf("!!!node suc ip;node suc port: %s %s\n",node->successor_ip,node->successor_port);
          printf("Send request_predecessor\n");
          cmd = strdup("Request_predecessor");
          printf("@: %s %s\n",node->ipAddr,node->port);
          sprintf(buf1,"%s %s %s\r\n", cmd,node->ipAddr,node->port);
        printf("@@buf1: %s\n",buf1);
          Rio_writen(serverfd,buf1,strlen(buf1));
          sleep(4);
        //}
        
    }
    return NULL;
}
/* listen stabilize */
void *listen_stabilize(void *args) {
    char *cmd, *old_predecessor_ip, *old_predecessor_port;
    char *saveptr, buf1[MAXLINE];
    int listenfd, connfd, clientfd, serverfd, clientlen, tries;
    //unsigned successor_id;
    struct sockaddr_in clientaddr;
    Node *node;
    rio_t client;
    
    printf("In listen stablize\n");
    node = ((Node **)args)[0];
    //node_port = strdup(node->port);
    /* predecessor_id,ip,port */
    //update_pre_id = node->predecessor_id;
    //update_pre_origin = strdup(node->predecessor_origin);
    //update_pre_ip = strdup(node->predecessor_ip);
    //update_pre_port = strdup(node->predecessor_port);
    /*
     successor_id = node->successor_id;
     successor_origin = strdup(node->successor_origin);
     successor_ip = strtok(successor_origin,":",&saveptr);
     successor_port = strtok_r(successor_origin,"\r\n",&saveptr);
     */
    
    listenfd = Open_listenfd(atoi(node->port)+5000);
    while(1) {
        printf("In listen stablize while\n");
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, &clientlen);
        clientfd = connfd;
        Rio_readinitb(&client, clientfd);
        printf("In listen stablize while success\n");
        Rio_readlineb(&client, buf1, MAXLINE);
        
        /* Determine cmd type */
        cmd = strtok_r(buf1," ",&saveptr);
        printf("cmd listen stablize: %s\n",cmd);
        if(strcmp(cmd,"Request_predecessor") == 0) {
            printf("In request predecessor\n");
            old_predecessor_ip = strtok_r(NULL," ",&saveptr);
            old_predecessor_port = strtok_r(NULL,"\r\n",&saveptr);
            printf("c1: %s %s\n",old_predecessor_ip,old_predecessor_port);
            
            tries = 3;
            while((serverfd = open_clientfd(old_predecessor_ip,atoi(old_predecessor_port)+5000))<0) {
                tries--;
                if(tries < 0) printf("Failed to open connection to the node already existed!\n");
            }
                cmd = strdup("Update_successor");
                sprintf(buf1,"%s %d %s %s\r\n", cmd,node->predecessor_id,node->predecessor_ip,node->predecessor_port);
                Rio_writen(serverfd,buf1,strlen(buf1));
                printf("write update_successor in listen stablize\n");
                printf("~~~new successor should be: %u %s %s\n",node->predecessor_id,node->predecessor_ip,node->predecessor_port);
        }
        else if(strcmp(cmd,"Update_successor") == 0) {
            printf("In update successor\n");
            unsigned tmp = atou(strtok_r(NULL," ",&saveptr));
            if(tmp != node->id) {
               node->successor_id = tmp;
               node->successor_ip = strtok_r(NULL," ",&saveptr);
               node->successor_port = strtok_r(NULL,"\r\n",&saveptr);
            }
            /* Node *node */
            notify_successor(node, node->successor_id, node->successor_ip, node->successor_port);
        }
        
    }
    return NULL;
}

/* fix fingers */
 void *fix_fingers(void *args) {
   Node *node = ((Node **)args)[0];
   while(1) {
       if(node->id != node->successor_id) {
         for(int i=0;i<KEY_BITS;i++) {
            start_find_successor(node,i,node->id,node->ipAddr,node->port);
         }
         sleep(4);
           printf("()()())()execute fix\n");
       }
   }
   return NULL;
 }
/* listen fix fingers */
void *listen_fix(void *args) {
    int listenfd, connfd, clientfd, clientlen;
    struct sockaddr_in clientaddr;
    Node *node;
    rio_t client;
    char buf1[MAXLINE], *cmd, *saveptr, *target_ip, *target_port;
    unsigned target_id;
    
    node = ((Node **)args)[0];
    listenfd = Open_listenfd(atoi(node->port)+10000);
    while(1) {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, &clientlen);
        clientfd = connfd;
        Rio_readinitb(&client, clientfd);
        printf("In listen fix fingers while success\n");
        Rio_readlineb(&client, buf1, MAXLINE);
        
        /* Determine cmd type */
        cmd = strtok_r(buf1," ",&saveptr);
        if(strcmp(cmd,"Results_of_start_successor") == 0) {
            printf("In result of start successor\n");
            /* cmd, successor_id, successor_origin */
            int i = atoi(strtok_r(NULL," ",&saveptr));
            node->finger_table.fingers[i].node->ipAddr = strtok_r(NULL," ",&saveptr);
            node->finger_table.fingers[i].node->port = strtok_r(NULL," ",&saveptr);
            printf("22results of find succ: %s %s\n",node->successor_ip,node->successor_port);
            node->finger_table.fingers[i].node->id = atou(strtok_r(NULL,"\r\n",&saveptr));
            
        }
        else if(strcmp(cmd,"find_start_successor") == 0) {
            printf("In find start successor\n");
            int j = atoi(strtok_r(NULL," ",&saveptr));
            target_ip = strtok_r(NULL," ",&saveptr);
            target_port = strtok_r(NULL," ",&saveptr);
            target_id = atou(strtok_r(NULL,"\r\n",&saveptr));
            /* find successor */
            printf("before call find start succ\n");
            start_find_successor(node,j,target_id,target_ip,target_port);
            
        }
    }
}

/* update finger search node of start */
void start_find_successor(Node *node, int num, unsigned target_id, char *target_ip, char *target_port) {
    start_find_predecessor(node,num,target_id,target_ip,target_port);
}

void start_find_predecessor(Node *node, int num, unsigned target_id, char *target_ip, char *target_port) {
    int tries,serverfd;
    char buf1[MAXLINE];
    char *cmd;
    
    if(!key_in_range(target_id, node->id, node->successor_id)) {
        start_closest_preceding_node(node, num, target_id, target_ip, target_port);
    }
    else {
        tries = 3;
        while((serverfd = open_clientfd(target_ip,atoi(target_port)))<0) {
            tries--;
            if(tries < 0) printf("Failed to open connection to the node already existed!\n");
        }
        cmd = strdup("Results_of_start_successor");
        sprintf(buf1,"%s %d %s %s %d\r\n",cmd,num,node->successor_ip,node->successor_port+10000,node->successor_id);
        printf("## %s\n",buf1);
        Rio_writen(serverfd,buf1,strlen(buf1));
    }
}
void start_closest_preceding_node(Node *node, int num, unsigned target_id, char *target_ip, char *target_port) {
    Finger finger1, finger2;
    int tries,serverfd;
    char buf1[MAXLINE];
    char *cmd;
    
    finger1 = node->finger_table.fingers[KEY_BITS-1];
    if(key_in_range(target_id, finger1.start, node->id)) {
        /* within the range, contact the corresponding node to find successor*/
        tries = 3;
        while((serverfd = open_clientfd(finger1.node->ipAddr,atoi(finger1.node->port)))<0) {
            tries--;
            if(tries < 0) printf("Failed to open connection to the node already existed!\n");
        }
        cmd = strdup("find_start_successor");
        sprintf(buf1,"%s %d %s %s %d\r\n", cmd,num,target_ip,target_port,target_id);
        Rio_writen(serverfd,buf1,strlen(buf1));
    }
    
    for(int i = KEY_BITS - 2; i >= 0; i--) {
        finger1 = node->finger_table.fingers[i];
        finger2 = node->finger_table.fingers[i+1];
        if(key_in_range(target_id, finger1.start, finger2.start)) {
            /* within the range, contact the corresponding node to find successor*/
            tries = 3;
            while((serverfd = open_clientfd(finger1.node->ipAddr,atoi(finger1.node->port)))<0) {
                tries--;
                if(tries < 0) printf("Failed to open connection to the node already existed!\n");
            }
            cmd = strdup("find_start_successor");
            sprintf(buf1,"%s %d %s %s %d\r\n", cmd,num,target_ip,target_port,target_id);
            Rio_writen(serverfd,buf1,strlen(buf1));
        }
    }
}

/* send heart-beating */
void *send_heart_beating(void *args) {
    char buf2[MAXLINE],*cmd;
    int serverfd, tries;
    Node *node;
    
    printf("in send heart\n");
    node = ((Node **)args)[0];
    /*unsigned predecessor_id = node->predecessor_id;
    char *predecessor_ip = node->predecessor_ip;
    char *predecessor_port = node->predecessor_port;
    unsigned node_id = node->id;
    char *node_ip = node->ipAddr;
    char *node_port = node->port;
    int *node_flag = node->flag;
    */
    printf("in send heartbeat pre_ip: %s\n",node->predecessor_ip);
    printf("pre_port: %s\n",node->predecessor_port);
    
    printf("x\n");
    while(1) {
        if(node->predecessor_id != node->id) {
            tries = 3;
            while((serverfd = open_clientfd(node->predecessor_ip,atoi(node->predecessor_port)+10000))<0) {
                tries--;
                if(tries < 0) printf("Failed to open connection to the node already existed!\n");
            }
        //printf("ready to send heartbeat\n");
        //printf("serverfd: %d\n",serverfd);
        
         
            printf("....in write heartbeat\n");
            *node->flag = 0;
            cmd = strdup("Keep_alive");
            sprintf(buf2,"%s %s %s\r\n",cmd,node->ipAddr,node->port);
            Rio_writen(serverfd,buf2,strlen(buf2));
            sleep(7);
            if(*(node->flag) != 1) {
                /* Handle the predecessor has left */
                printf("in some node dropped\n");
            }
        }
    }
    return NULL;
}
/* listen heart-beating */
void *listen_heart_beating(void *args) {
    char *saveptr, buf2[MAXLINE];
    int listenfd, connfd, clientfd, serverfd, clientlen, tries;
    struct sockaddr_in clientaddr;
    char *node_port, *successor_ip, *successor_port, *cmd;
    Node *node;
    int *node_flag;
    rio_t client;
    
    printf("in listen heart\n");
    node = ((Node **)args)[0];
    node_port = node->port;
    node_flag = node->flag;
    printf("node_port: %s\n",node_port);
    listenfd = Open_listenfd(10000+atoi(node_port));
    printf("listenfd: %d\n",listenfd);
    while(1) {
        printf("in while listen heart\n");
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, &clientlen);
        clientfd = connfd;
        Rio_readinitb(&client, clientfd);
        printf("in while listen heart success\n");
        Rio_readlineb(&client, buf2, MAXLINE);
        
        /*printf("ready to listen heart\n");
        if(node->successor_ip != node->ipAddr && node->successor_port != node->port) {
          printf("tingjian\n");
          tries = 3;
          while((serverfd = open_clientfd(node->successor_ip,atoi(node->successor_port)+10000))<0) {
              tries--;
              if(tries < 0) printf("Failed to open connection to the node already existed!\n");
          }
        }
        */
        /* Determine cmd type */
        cmd = strtok_r(buf2," ",&saveptr);
        printf("listen heart cmd\n");
        printf("listen heartbeat..cmd: %s\n",cmd);
        if(strcmp(cmd,"Keep_alive") == 0) {
            printf("in receive Keep_alive\n");
            successor_ip = strtok_r(NULL," ",&saveptr);
            successor_port = strtok_r(NULL,"\r\n",&saveptr);
            
            /*if(successor_ip != node->successor_ip && successor_port != node->successor_port) {
                tries = 3;*/
                while((serverfd = open_clientfd(successor_ip,atoi(successor_port)+10000))<0) {
                    tries--;
                    if(tries < 0) printf("Failed to open connection to the node already existed!\n");
                }
                /*node->successor_ip = successor_ip;
                node->successor_port = successor_port;
            }*/
            cmd = strdup("Alive ");
            sprintf(buf2,"%s\r\n", cmd);
            printf("buf2: %s",buf2);
            Rio_writen(serverfd,buf2,strlen(buf2));
            printf("write alive\n");
        }
        else if(strcmp(cmd,"Alive") == 0) {
            printf("!!!receive alive\n");
            *(node_flag) = 1;
        }
    }
    return NULL;
}

/* Search */
void node_find_successor(Node *node, unsigned target_id, char *target_ip, char *target_port) {
    printf("in find succ func\n");
    node_find_predecessor(node,target_id,target_ip,target_port);
}

void node_find_predecessor(Node *node, unsigned target_id, char *target_ip, char *target_port) {
    int tries,serverfd;
    char buf1[MAXLINE];
    char *cmd;
    
    printf("llllllllll target_id,node_id,node_uscces_id: %u %u %u\n",target_id,node->id,node->successor_id);
    printf("in find prede func\n");
    if(!key_in_range(target_id, node->id, node->successor_id)) {
        printf("ready to find cloeset\n");
        node_closest_preceding_node(node, target_id, target_ip, target_port);
    }
    else {
        tries = 3;
        while((serverfd = open_clientfd(target_ip,atoi(target_port)))<0) {
            tries--;
            if(tries < 0) printf("Failed to open connection to the node already existed!\n");
        }
        printf("ready to send result of successor\n");
        cmd = strdup("Results_of_find_successor");
        sprintf(buf1,"%s %s %s %d\r\n",cmd,node->successor_ip,node->successor_port,node->successor_id);
        Rio_writen(serverfd,buf1,strlen(buf1));
    }
}
void node_closest_preceding_node(Node *node, unsigned target_id, char *target_ip, char *target_port) {
    Finger finger1, finger2;
    int tries,serverfd;
    char buf1[MAXLINE];
    char *cmd;
    
    printf("aaaain find cloeset\n");
    printf("targetid,nodeid,successorid: %u %u %u\n",target_id,node->id,node->successor_id);
    finger1 = node->finger_table.fingers[KEY_BITS-1];
    if(key_in_range(target_id, finger1.start, node->id)) {
        /* within the range, contact the corresponding node to find successor*/
        // {
        if(finger1.node->ipAddr != node->ipAddr) {
        tries = 3;
        while((serverfd = open_clientfd(finger1.node->ipAddr,atoi(finger1.node->port)))<0) {
            tries--;
            if(tries < 0) printf("Failed to open connection to the node already existed!\n");
        }
        printf("Found it\n");
        cmd = strdup("find_successor");
        sprintf(buf1,"%s %s %s %d\r\n", cmd,target_ip,target_port,target_id);
        Rio_writen(serverfd,buf1,strlen(buf1));
        }
        //}
    }
    else {
    for(int i = KEY_BITS - 2; i >= 0; i--) {
        printf("In compare with fingertable\n");
        finger1 = node->finger_table.fingers[i];
        finger2 = node->finger_table.fingers[i+1];
        if(key_in_range(target_id, finger1.start, finger2.start)) {
            /* within the range, contact the corresponding node to find successor*/
            printf("Found it!\n");
            tries = 3;
            while((serverfd = open_clientfd(finger1.node->ipAddr,atoi(finger1.node->port)))<0) {
                tries--;
                if(tries < 0) printf("Failed to open connection to the node already existed!\n");
            }
            cmd = strdup("In fingertable! find_successor");
            sprintf(buf1,"%s %s %s %d\r\n", cmd,target_ip,target_port,target_id);
            Rio_writen(serverfd,buf1,strlen(buf1));
        }
    }
    }
}
int key_in_range(unsigned target_id, unsigned node_id, unsigned node_successor_id) {
    if(target_id < node_successor_id && target_id > node_id && node_id < node_successor_id) return 1;
    else if(node_id > node_successor_id  && (target_id > node_id || target_id < node_successor_id)) return 1;
    else if(node_id == node_successor_id) return 1;
    else return 0;
}




/* Main function for the program */
int main(int argc, char *argv[]) {
    /* variable declarations */
    char *port_to_join, *ip_in_ring, *port_in_ring;
    
    
    /* implementation */
    if(argc < 2) {
        printf("Usage: %s input error!\n", argv[0]);
        exit(1);
    }
    /* quit: ./quit */
    if(argc == 2) {
        
        
    }
    /* create */
    else if(argc == 3) {
        /* each thread grab a lock */
        //pthread_mutex_init(&mutex,NULL);
        
        port_to_join = argv[2];
        ip_in_ring = strdup("0");
        port_in_ring = strdup("-1");
        node_create(port_to_join, ip_in_ring, port_in_ring);
        
        //Pthread_detach(tid);
        
        //pthread_mutex_destroy(&mutex);
    }
    /* join */
    else if(argc == 5) {
        if(strcmp(argv[1],"chord") != 0) {
            printf("Input error!\n");
        }
        else {
            //pthread_mutex_init(&mutex,NULL);
            
            port_to_join = argv[2];
            ip_in_ring = argv[3];
            port_in_ring = argv[4];
            
            node_create(port_to_join, ip_in_ring, port_in_ring);
            
            //Pthread_detach(tid);
        
            //pthread_mutex_destroy(&mutex);
        }
    
   
    
    }
}


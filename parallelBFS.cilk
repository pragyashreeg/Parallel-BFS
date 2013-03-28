#include <iostream>
#include <string>
#include <sstream>
#include <pthread_mutex.h>
#include <cilk.h>
#include <reducer_max.h>
#include <reducer_opadd.h>
#include <cstdlib>
#include <pthread.h>
#include <cilkview.h>
#define NUMPROC 20
#define MAX_STEAL_ATTEMPTS 5 
#define MIN_STEAL_SIZE 40

using namespace std;

cilk::mutex mtx[NUMPROC+1];

struct AdQ {
       int vert;
       AdQ *next;
};

typedef AdQ AdQ_t;

class Queue{
      
 public:
      AdQ *head;
      AdQ *tail;
      int count;
      
      Queue();
      void enqueue(long n);
      long dequeue();
      void printQueue();
      bool isEmpty();
};

class QueQueue{
 public:
      Queue **q;
      QueQueue();
      Queue* getQueue(int i);
      void enqueueIntoQueue(int i,long element);
      long dequeueFromQueue(int i);
      bool isEmpty();
      void setQueue(int i, Queue *Q);
};

class Graph{
      Queue **adjacencyMatrix;
      long *sourceVertices;
    public:
      long numVertices;
      long numEdges;
      long numSources;
      Graph(long n,long);
      long getSource(long i);
      void pointToQueue(long vertice,Queue *q);
      Queue * getAdjacent(long vertice);
      void printQueue(long vertice);
      void inputSources(long vertice,long index);
      void printSources();
      void serialBFS(long sourceVertex,FILE *fp);
      void parallelBFS(long s, FILE *fp);
      void parallelBFSThread(int i,QueQueue *Qout,long *d,QueQueue *Qin, FILE *fp);
      unsigned long long computeChecksum( long *d ,FILE *fp );
      unsigned long long computeChecksumSerial( long *d ,FILE *fp );
};

class FileParser{
    public:
          FILE *fp;
          long* getNumbers(char *str, int n);
          Graph getAdjacency();
};

void QueQueue::setQueue(int i, Queue *Q){
	q[i] = Q;
}

long Graph::getSource(long i){
	return this->sourceVertices[i];
}

bool QueQueue::isEmpty(){
     bool isEmpty = true;
     for(int i = 1;i<=NUMPROC; i++)
             if(q[i] != NULL && q[i]->count > 0) isEmpty = false;
     if(isEmpty)
     return true;
     else return false;
}

QueQueue::QueQueue(){
    q = (Queue **)malloc(sizeof(Queue **) * (NUMPROC+1));
    for(int i = 1; i<= NUMPROC ; i++)
    {
            q[i] = NULL;
    }
}

Queue * QueQueue::getQueue(int i){
      return q[i];
}

void QueQueue::enqueueIntoQueue(int i,long element){
     	if(q[i] == NULL)
	{
		q[i] = new Queue();
	}
	q[i]->enqueue(element);
}

long QueQueue::dequeueFromQueue(int i){
     if(q[i] == NULL) return -1;
     long l;
     if(q[i] != NULL)
	 l = q[i]->dequeue();
     //if(l < 0) q[i] = NULL;
     if(q[i]->head == NULL){
	Queue *temp = q[i];
	q[i] = NULL;
	free(temp);
     }
     return l;
}

Queue::Queue(){
      head = NULL;
      tail = NULL;
      count = 0;
}

void Queue::enqueue(long n){
     
      AdQ_t *newNode = (AdQ_t *)malloc(sizeof(AdQ_t *));
      newNode->vert = n;
      newNode->next = NULL;
      
      if(this->head == NULL) {
                   this->head = newNode;
                   this->tail = newNode;
		   this->count++;
                   return;
      }
      
      this->tail->next = newNode;
      this->tail = newNode;
      this->count++;
}

void Queue::printQueue(){
     AdQ_t *temp = this->head;
     
     while(temp!=NULL){
                      printf("%d ",temp->vert);
                      temp = temp->next;
     }
     printf("\n");
}

void Graph::printQueue(long vertice){
     Queue *c;
     c = getAdjacent(vertice);
     if(c != NULL)
     c->printQueue();
     else printf("\n");
}

long Queue::dequeue(){
     if(head == NULL){
             //printf("Queue empty");
             return -1;
     }
     AdQ *temp = head;
     long data = -1;
     if(temp == NULL) return -1;
     if(temp!= NULL)data = temp->vert;
     if(head != NULL)head = head->next;
     count--;
     if(head == NULL) tail = NULL;
     free(temp);
     return data;
}

bool Queue::isEmpty(){
     if(head == NULL) return true;
     return false;
}

Graph::Graph(long numVertices,long numSources){
  adjacencyMatrix = (Queue **)malloc(sizeof(Queue **)*(numVertices+1));
  sourceVertices = (long *)malloc(sizeof(long *)*(numSources+1));
  for(long i = 0;i<= numVertices; i++)
          adjacencyMatrix[i] = NULL;
  for(long i = 0;i<= numSources; i++)
          sourceVertices[i] = 0;
}

void Graph::pointToQueue(long vertice,Queue *q){
     adjacencyMatrix[vertice] = q;
}

Queue * Graph::getAdjacent(long vertice){
      if( vertice < 1 || vertice > numVertices){
	 return NULL;
	}
      return adjacencyMatrix[vertice];
}

//Parse a line to get the numbers
long* FileParser::getNumbers(char *str, int n){
    long *arr = (long *)malloc(sizeof(long *)* n);
    char *temp = str;
    std::istringstream is( str );
    long i = 0;
    long k;
    while( is >> k ) {
           arr[i] = k;
           i++;
    }
    return arr;
}

void Graph::inputSources(long vertice,long index){
     sourceVertices[index] = vertice;
}

void Graph::printSources(){
     for(long i = 0;i< numSources;i++)
     cout << sourceVertices[i] << " ";
     cout<< endl;
}

unsigned long long Graph::computeChecksumSerial( long *d , FILE *fp )
{
         long maxDist = 0;
         for (long i = 1; i<= numVertices; i++){
		if(d[i] > maxDist && d[i] != numVertices) maxDist = d[i];
	 }

         unsigned long long chksum = 0;
         for ( int i = 1; i <= numVertices; i++ )
                  chksum += d[i];
	 cout << maxDist << " " << chksum << endl;
	 //fprintf(fp,"%ld %ld\n", maxDist, chksum);
         return chksum;
}

//Run bfs on a node for a graph
void Graph::serialBFS(long sourceVertex, FILE *fp){

    long *d = (long *)malloc(sizeof(long *)*(numVertices+1));
    for(long i = 1; i<=numVertices;i++)
             d[i] = numVertices; //LONG_MAX;
            
    d[sourceVertex] = 0;
    Queue Q;
    Q.enqueue(sourceVertex);

    while(!Q.isEmpty()){
                   long u = Q.dequeue();
                   Queue *c;
                   c = getAdjacent(u);
		   if(c == NULL) continue;		
                   AdQ_t *adjacentNodes = c->head;
                   while(adjacentNodes!=NULL){
                                    if(d[adjacentNodes->vert] == numVertices){
                                                      d[adjacentNodes->vert] = d[u] + 1;
                                                      Q.enqueue(adjacentNodes->vert);
                                    }
                          adjacentNodes = adjacentNodes->next;
                   }
    }
    computeChecksumSerial(d,fp);
    //free(d);
}

Graph FileParser::getAdjacency(){
    char str[200];
    //fp = fopen("samples/sample-01-in.txt","r");
    fp = fopen("/work/01905/rezaul/CSE638/HW1/turn-in/cage15-in.txt","r");
    //fp = fopen("testFiles/cage14-in.txt","r");
    //fp = fopen("/work/01905/rezaul/CSE638/HW1/turn-in/wikipedia-in.txt", "r");
    
    if(!fp){
            cout << "File not found, exiting "<< endl;
             Graph g(0,0);
             return g;
    }
    
    fgets(str,sizeof(str),fp);
    long *num = getNumbers(str,3);
    long numVertices = num[0];
    long numEdges = num[1];
    long numSources = num[2];
    long edgeCounter = numEdges;
    long sourceCounter = 0;
    Graph g(numVertices,numSources);
    g.numVertices = numVertices;
    g.numEdges = numEdges;
    g.numSources = numSources;                                                                                    
 
    long currentNum = -1;
    long oldNum = -1;
                                                                                                    
    Queue *q;
    q = new Queue();
    while(edgeCounter > 0 && fgets(str,sizeof(str),fp) != NULL)
    {
     int len = strlen(str)-1;
     if(str[len] == '\n') 
         str[len] = 0;
      num = getNumbers(str,2);
      currentNum = num[0];
      
      if(currentNum != oldNum)
      {
                    if(oldNum != -1){
                              g.pointToQueue(oldNum,q);
                              q = new Queue();
                                                                                                    
                    }
                    oldNum = currentNum;
      }
      q->enqueue(num[1]);
                                                                                                    
      edgeCounter--;
    }
    g.pointToQueue(oldNum,q);
                                                                                                    
    
    while(fgets(str,sizeof(str),fp) != NULL && sourceCounter <= numSources )
    {
     int len = strlen(str)-1;
     if(str[len] == '\n') 
         str[len] = 0;
      num = getNumbers(str,1);
      g.inputSources(num[0],sourceCounter);
      sourceCounter++;
    }
    
    fclose(fp);
    
    return g;
}

unsigned long long Graph::computeChecksum( long *d , FILE *fp )
{
         cilk::reducer_opadd< unsigned long long > chksum;
         cilk_for ( long i = 1; i <= numVertices; i++ )
                  chksum += d[i];

         cilk_for ( long i = 1; i <= numVertices; i++ )
                  if(d[i] == numVertices) d[i] = 0;

         cilk::reducer_max<long> maxVal;
         cilk_for (long i = 1; i <= numVertices; i++)
         cilk::max_of(maxVal,d[i]);
	 //fprintf(fp,"%ld %ld\n", maxVal.get_value(), chksum.get_value());
	 cout << maxVal.get_value() << " " << chksum.get_value() << endl;
         //printf("%ld %ld\n",maxVal.get_value(),chksum.get_value());
}

void Graph::parallelBFSThread(int i,QueQueue *Qout,long *d,QueQueue *Qin , FILE *fp)
{
   while(1){
              while(Qin->getQueue(i) != NULL)
	      {
			 mtx[i].lock(); 
                         long u = Qin->dequeueFromQueue(i);
			 mtx[i].unlock();
			 if(u == -1) break; 
			 Queue *c;
                         c = getAdjacent(u);
			 if(c == NULL) continue;
                         AdQ_t *adjacentNodes = NULL;
			 if(c != NULL) adjacentNodes = c->head;
                         while(adjacentNodes!=NULL){
                         if(d[adjacentNodes->vert] == numVertices){
				      d[adjacentNodes->vert] = d[u] + 1;
                                      Qout->enqueueIntoQueue(i,adjacentNodes->vert);
				      
                         }//if
                         adjacentNodes = adjacentNodes->next;
                   	 }//adjacent
              }//!Qin
       int t = 0;
       mtx[i].lock();
       while(Qin->getQueue(i) == NULL && t < MAX_STEAL_ATTEMPTS){
	   int r = rand() % NUMPROC + 1;
	   if(r!= i && mtx[r].try_lock() == 1){
	   Queue *victim;
           if(( victim = Qin->getQueue(r)) != NULL){
                 if(victim!= NULL && victim->count > MIN_STEAL_SIZE)
                 {
                             AdQ *fast = NULL;
			     if(victim != NULL)fast = Qin->getQueue(r)->head;
                             AdQ *slow = fast;
                             while(fast != NULL && slow != NULL && fast->next != NULL){
                             	if(slow!=NULL)slow = slow->next;
				if(fast!= NULL && fast->next != NULL)fast = fast->next->next;
                             }//While
                             if(slow!=NULL && victim->count > MIN_STEAL_SIZE){
				AdQ *nextHead = slow->next;
                             	if(slow!=NULL)slow->next = NULL;
			     	Queue *newQueue = new Queue();
			     	if(newQueue != NULL)newQueue->head = nextHead;
				if(Qin != NULL)Qin->setQueue(i,newQueue);
				//if(newQueue != NULL)Qin->q[i] = newQueue;
				//cout<<"Stolen " << (Qin->q[i] == NULL ) << endl;
			     }
                  }//if(Qin
	   }
           mtx[r].unlock();
	   }
           t++;      
     }//While(Qin isEmpty
     mtx[i].unlock();
     if(Qin->getQueue(i) == NULL)break;
  }
}

void Graph::parallelBFS(long s, FILE *fp){
     long *d = (long *)malloc(sizeof(long *) * (numVertices + 1));
     cilk_for(long i = 1; i<= numVertices; i++)
                   d[i] = numVertices;
     d[s] = 0;
     int p = NUMPROC;
     QueQueue *Qin, *Qout;
     Qin = new QueQueue();
      
     Qin->enqueueIntoQueue(1,s);

     while(!Qin->isEmpty()){    
           Qout = new QueQueue();
           for(int i = 1; i< p; i++)
           {
                   cilk_spawn parallelBFSThread(i,Qout,d,Qin,fp);
           }
           parallelBFSThread(p,Qout,d,Qin,fp);
           cilk_sync;
           Qin = Qout;
     }
    computeChecksum(d, fp);
    free(d);
}

int cilk_main(){

    FileParser f;
    FILE *fp;
    fp = fopen("wiki_p_out_s.txt", "w");
    Graph g = f.getAdjacency();
    
    clock_t start = clock();
/*    for(long i = 0; i < g.numSources; i++)
	g.serialBFS(g.getSource(i),fp);
*/

    cilk::cilkview cv;
    for(long i = 0; i < g.numSources; i++){
	cv.start();
	g.parallelBFS(g.getSource(i),fp);
	cv.stop();
	cv.dump("graphwiki");
    }
    clock_t end = clock();

    float seconds = (float)(end - start)/CLOCKS_PER_SEC;
    //fprintf(fp,"%f", seconds);    
    //cout << seconds << endl;
    fclose(fp);
    return 0;
}

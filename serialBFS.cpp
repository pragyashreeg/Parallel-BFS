#include <iostream.h>
#include <reducer_opadd.h>
#define P 8
using namespace std;
int n, m, r;
typedef struct list{
	int index;
	struct list *next;
} AdjList;

typedef struct node{
	int index;
	AdjList *start;
	AdjList *adjList;
	struct node *next;
} Node;

typedef struct{
	int src;
	int maxBFSLevel;
} srcMaxBFSLevel;

// Class Queue
class Queue {
	private:		
		Node *start;
		Node *end;
	public:
		Queue()
		{
			start = NULL;
			end = NULL;
		}
		void enque( Node *);
		Node * deque();	
		bool isEmpty();
		void traverseQ();
};

// Class Queue :: Function Definations
void Queue :: enque( Node *v )
{
	Node *temp = NULL, *prev = NULL;
	if( start == NULL )
	{
		start = v;
		end = v;
		v->next = NULL;
		return;
	}
	end->next = v;
	end = end->next;
	end->next = NULL;
}

Node * Queue :: deque()
{
	Node *v = NULL;
	if( start == NULL )
		return v;
	v = start;
	if( start->next == NULL )
	{
		start = NULL;
		end = NULL;
	}
	else
	{
		start = start->next;
	}
	v->next = NULL;
	return v;
}

bool Queue :: isEmpty()
{
	if( start == NULL )
		return true;
	else 
		return false;
}
void Queue :: traverseQ() 
{       
	cout << "\nTraversing Q "<<endl;        
        Node * temp = start;
        while( temp != NULL)
        {
		cout << temp->index << " -> ";
                temp = temp->next;
        }
}
 
// Function Serial-BFS
void Serial_BFS( Node *V, srcMaxBFSLevel *Src, int *d)
{
	int index, i, src, maxBFSLevel = 0;
	Node *u = NULL, *v = NULL;
	for( i = 1; i <= n; i++ )
	{
		d[i] = m*5;
	}
	
	Queue Q;
	src = Src->src;
	d[src] = 0;
	Q.enque(V+src);
	int level = 0;
	while( Q.isEmpty() == false)
	{
		level = level + 1;
		u = Q.deque();
		u->adjList = u->start;
		while( u->adjList != NULL)
		{
			index = u->adjList->index;
			if( d[index] == m*5 )
			{
				d[index] = d[u->index] + 1;
				Q.enque(V+index);
			}
			if( maxBFSLevel < d[index] )
				maxBFSLevel = d[index];		
			u->adjList = u->adjList->next;
		}
	}
	Src->maxBFSLevel = maxBFSLevel;
	return; 
}

unsigned long long computeChecksum( int * d )
{               
        cilk::reducer_opadd< unsigned long long > chksum;
        cilk_for ( int i = 1; i <= n; i++ )
        {       
                if( d[ i ] == m*5 )
                        d[ i ] = n;
                chksum += d[ i ];
        }               
	return chksum.get_value( );     
} 

int cilk_main()
{
	FILE *fp, *fpout;
	char buff[100];
	int i, u, v;
	int maxBFSLevel;
	//fp = fopen("sample-01-in.txt","r");
	//fp = fopen("InputFiles/cage14-in.txt", "r");
	//fp = fopen("InputFiles/cage15-in.txt", "r");
	//fp = fopen("InputFiles/freescale-in.txt", "r");
	//fp = fopen("InputFiles/kkt_power-in.txt", "r");
	//fp = fopen("InputFiles/rmat100M-in.txt", "r");
	//fp = fopen("InputFiles/rmat1B-in.txt", "r");
	fp = fopen("InputFiles/wikipedia-in.txt", "r");
	fscanf( fp, "%s", buff);
	n = atoi(buff);
	fscanf( fp, "%s", buff);
	m = atoi(buff);
	fscanf( fp, "%s", buff);
	r = atoi(buff);
	//cout << "#nodes: " << n << endl;
	//cout << "#edges: " << m << endl;
	//cout << "#source vertices: " << r << endl;
	int *d = (int *) malloc( (n+1)* sizeof(int) );
	Node *V = (Node *) malloc( (n+1)* sizeof(Node) );
	srcMaxBFSLevel *Src = (srcMaxBFSLevel *) malloc( (r+1) * sizeof(srcMaxBFSLevel) );
	for( i = 1; i <= n; i++ )
	{
		V[i].index = i;
		V[i].start = NULL;
		V[i].adjList = NULL;
		V[i].next = NULL;
	}
	for( i = 1; i <= m; i++)
	{	
		fscanf( fp, "%s", buff);
		u = atoi(buff);
		fscanf( fp, "%s", buff);
		v = atoi(buff);
		if( V[u].start == NULL )
		{
			V[u].adjList = (AdjList*)malloc( sizeof(AdjList));
			V[u].adjList->index = v;
			V[u].adjList->next = NULL;
			V[u].start = V[u].adjList;
		}
		else
		{
			V[u].adjList->next = (AdjList*)malloc( sizeof(AdjList));
			V[u].adjList = V[u].adjList->next;
			V[u].adjList->index = v;
			V[u].adjList->next = NULL;
		}
	}
	for( i = 1; i <= r; i++ )
	{
		fscanf( fp, "%s", buff);
		int s = atoi(buff);
		Src[i].src = s;
		Src[i].maxBFSLevel = 0;
	}	
	fclose(fp);

	//fpout = fopen("OutputFiles/cage14-out.txt","w");
	//fpout = fopen("OutputFiles/cage15-out.txt","w");
	//fpout = fopen("OutputFiles/freescale-out.txt","w");
	//fpout = fopen("OutputFiles/kkt_power-out.txt","w");
	//fpout = fopen("OutputFiles/rmat100M-out.txt","w");
	//fpout = fopen("OutputFiles/rmat1B-out.txt","w");
	fpout = fopen("OutputFiles/wikipedia-out.txt","w");
	unsigned long long cksum = 0;
	for(i=1; i <= r; i++)
	{
		Serial_BFS( V, Src+i, d );
                cksum = computeChecksum( d );
		//cout <<  Src[i].maxBFSLevel<< cksum;
                fprintf(fpout,"%d %llu\n", Src[i].maxBFSLevel, cksum);
	}
	fclose(fpout);
return 0;
}


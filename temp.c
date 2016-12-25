#include <stdio.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>

typedef struct LPair {
  int d_key;
  int d_tid;       /* tuple ID */
};

/* a B+-Tree internal node of 64 bytes.
   corresponds to a cache line size of 64 bytes.
   We can store a maximum of 7 keys and 8 child pointers in each node. */
typedef struct BPLNODE64 {
  int        d_num;       /* number of keys in the node */
  void*      d_flag;      /* this pointer is always set to null and is used to distinguish
			     between and internal node and a leaf node */
  struct BPLNODE64* d_prev;      /* backward and forward pointers */
  struct BPLNODE64* d_next;
  struct LPair d_entry[6];       /* <key, TID> pairs */
};

/* a CSB+-Tree internal node of 64 bytes.
   corresponds to a cache line size of 64 bytes.
   We put all the child nodes of any given node continuously in a node group and
   store explicitly only the pointer to the first node in the node group.
   We can store a maximum of 14 keys in each node.
   Each node has a maximum of 15 implicit child nodes.
*/
typedef struct CSBINODE64 {
  int    d_num;
  void*  d_firstChild;       //pointer to the first child in a node group
  int    d_keyList[14];
   struct CSBINODE64 * next;
//public:
  //int operator == (const CSBINODE64& node) {
    //if (d_num!=node.d_num)
      //return 0;
    //for (int i=0; i<d_num; i++)
      //if (d_keyList[i]!=node.d_keyList[i])
        //return 0;
    //return 1;
  //}
};
struct CSBINODE64*    g_csb_root64;







// This version of bulkload allocates a full nodegroup.
// The benefit of doing that is, we never need to deallocate space.
// Every time we have to split, we simply allocate another full nodegroup and
// redistribute the nodes between the two node groups.
// bulk load a CSB+-Tree
// n: size of the sorted array
// a: sorted leaf array
// iUpper: maximum number of keys for each internal node duing bulkload.
// lUpper: maximum number of keys for each leaf node duing bulkload.
// Note: iUpper has to be less than 14.
void csbBulkLoad641(int n, struct LPair* a, int iUpper, int lUpper) {
  struct BPLNODE64 *lcurr, *start, *lprev;
  struct CSBINODE64 *iLow, *iHigh, *iHighStart, *iLowStart;
  int temp_key;
  void* temp_child;
  int i, j, k, l, nLeaf, nHigh, nLow, remainder, nHigh1, nLow1, nLeaf1;

  // first step, populate all the leaf nodes
  nLeaf=(n+lUpper-1)/lUpper;
//  printf("nLeaf value is %d \n", nLeaf);
  nLeaf1=((int)(nLeaf+iUpper)/(iUpper+1))*15;                 // allocate full node group
  lcurr=(struct BPLNODE64*) malloc(sizeof(struct BPLNODE64)*nLeaf1);
  lcurr->d_flag=0;
  lcurr->d_num=0;
  lcurr->d_prev=0;
  start=lcurr;
  k=0;
  for (i=0; i<n; i++) {
    if (lcurr->d_num >= lUpper) { // at the beginning of a new node

      k++;
      lprev=lcurr;
      if (k==(iUpper+1)) {
	for (l=iUpper+2; l<=(14+1); l++) { // pad empty nodes
	  lcurr++;
	  lcurr->d_flag=0;
	  lcurr->d_num=-1;
	}
	k=0;
      }
      lcurr++;
      lcurr->d_flag=0;
      lcurr->d_num=0;
      lcurr->d_prev=lprev;

      lprev->d_next=lcurr;
    }
    lcurr->d_entry[lcurr->d_num]=a[i];
    lcurr->d_num++;
  }
  lcurr->d_next=0;


  lcurr++;
  while (lcurr < start+nLeaf1) {
    lcurr->d_flag=0;
    lcurr->d_num=-1;
    lcurr++;
  }
// Leaf nodes all checks
  //int u;

 // for(u=0;u<5;u++){
//printf ("lcurr values are %d \n",lprev->d_next->d_entry[u]);
 // }
  // second step, build the internal nodes, level by level.
  // we can put IUpper keys and IUpper+1 children (implicit) per node
  //printf("nLeaf value is %d \n", nLeaf);
  nHigh=(nLeaf+iUpper)/(iUpper+1);
 // printf("nHigh value is %d \n ",nHigh);
  remainder=nLeaf%(iUpper+1); // remainder =0;
  nHigh1=((int)(nHigh+iUpper)/(iUpper+1))*15;
  iHigh=(struct CSBINODE64*) malloc(sizeof(struct CSBINODE64)*nHigh1);
  //printf("nHigh1 value is %d\n ",nHigh1);
  iHigh->d_num=0;
  iHigh->d_firstChild=start;
  iHighStart=iHigh;
  lcurr=start;
  for (i=0; i<((remainder==0)?nHigh:(nHigh-1)); i++) { // baad mai
    iHigh->d_num=iUpper;
    iHigh->d_firstChild=lcurr;
    for (j=0; j<iUpper+1; j++) {

      iHigh->d_keyList[j]=(lcurr+j)->d_entry[(lcurr+j)->d_num-1].d_key;
      //printf("iHigh value is %d\n");
      //int u;
      //for(u=0;u<=5;u++){
      //printf("iHigh key list %d th values is %d\n", u,iHigh->d_keyList[u]);
      //}
    }
    lcurr+=15;

    iHigh++;
    if (i%(iUpper+1)+1==iUpper+1) {      //skip the reserved empty nodes

      iHigh+=14-iUpper;
    }
  }
  if (remainder==1) {
    //this is a special case, we have to borrow a key from the left node if there is one
    //leaf node remaining.
    if (nHigh%(iUpper+1)==1) {   //find previous node in the previous node group
      iHigh->d_keyList[0]=(iHigh-1-(14-iUpper))->d_keyList[iUpper];
      (iHigh-1-(14-iUpper))->d_num--;
    }
    else { //find previous node in the same node group
    	printf("inside else");
      iHigh->d_keyList[0]=(iHigh-1)->d_keyList[iUpper];
      (iHigh-1)->d_num--;
    }
    iHigh->d_num=1;
    iHigh->d_firstChild=lcurr;
    lcurr[1]=lcurr[0];
    lcurr[0]=*(lcurr-1-(14-iUpper));
    (lcurr-1-(14-iUpper))->d_num=-1;

    iHigh++;
    lcurr+=15;
  }
  else if (remainder>1) {

    iHigh->d_firstChild=lcurr;
    for (i=0; i<remainder; i++) {

      iHigh->d_keyList[i]=(lcurr+i)->d_entry[(lcurr+i)->d_num-1].d_key;
    }
    iHigh->d_num=remainder-1;

    iHigh++;
    lcurr+=15;
  }



  while (nHigh>1) {

    nLow=nHigh;
    nLow1=nHigh1;
    iLow=iHighStart;
    iLowStart=iLow;
    nHigh=(nLow+iUpper)/(iUpper+1);
    //printf("nHigh value is %d",nHigh);
    remainder=nLow%(iUpper+1);

    if (nHigh>1)
      nHigh1=((int)(nHigh+iUpper)/(iUpper+1))*15;
    else
      nHigh1=nHigh;
    iHigh=(struct CSBINODE64*) malloc(sizeof(struct CSBINODE64)*nHigh1);
    iHigh->d_num=0;
    iHigh->d_firstChild=iLow;
    iHighStart=iHigh;
    for (i=0; i<((remainder==0)?nHigh:(nHigh-1)); i++) {
      iHigh->d_num=iUpper;
      iHigh->d_firstChild=iLow;
      for (j=0; j<iUpper+1; j++) {

	iHigh->d_keyList[j]=(iLow+j)->d_keyList[(iLow+j)->d_num];
	//int u;
	  //    for(u=0;u<=5;u++){
	    //  printf("iHigh key list %d th values is %d\n", u,iHigh->d_keyList[u]);
	     // }
      }
      iLow+=15;

      iHigh++;
      if (i%(iUpper+1)+1==iUpper+1) {      //skip the reserved empty nodes

	iHigh+=14-iUpper;
      }
    }
    if (remainder==1) { //this is a special case, we have to borrow a key from the left node
      if (nHigh%(iUpper+1)==1) {   //find previous node in the previous node group
	iHigh->d_keyList[0]=(iHigh-1-(14-iUpper))->d_keyList[iUpper];
	(iHigh-1-(14-iUpper))->d_num--;
      }
      else { //find previous node in the same node group
	iHigh->d_keyList[0]=(iHigh-1)->d_keyList[iUpper];
	(iHigh-1)->d_num--;
      }
      iHigh->d_num=1;
      iHigh->d_firstChild=iLow;
      iLow[1]=iLow[0];
      iLow[0]=*(iLow-1-(14-iUpper));
      (iLow-1-(14-iUpper))->d_num=-1;

      iHigh++;
      iLow+=15;
    }
    else if (remainder>1) {
      iHigh->d_firstChild=iLow;
      for (i=0; i<remainder; i++) {

	iHigh->d_keyList[i]=(iLow+i)->d_keyList[(iLow+i)->d_num];
      }
      iHigh->d_num=remainder-1;

      iHigh++;
      iLow+=15;
    }


  }

  g_csb_root64=iHighStart;
  int p;
  for(p=0;p<5;p++){
  printf("%d values are %d\n",p,iHighStart->d_keyList[p]);

  }

}


int array[] = {2,3,5,7,12,13,16,19,20,22,24,25,27,30,31,33,36,39};
struct LPair* a;
struct CSBINODE64* root;

	void main(){
	    int i =0;
	    int iUpper = 2;
	    int lUpper = 2;
	    int count =(int)( sizeof(array) / sizeof(array[0]));
	    //printf("hello\n");
	    //printf ("count is %d\n",count);
	    a = (struct LPair*)malloc(sizeof(struct LPair)*count);
	    for(i =0;i<count;i++){
	        a[i].d_key = array[i];
	        a[i].d_tid = array[i];
	     //   printf("%d\t",a[i].d_key);
	    }
	   csbBulkLoad641(count,a,iUpper,lUpper);

	   // printf ("%d\n",root->d_keyList[2]);

	}




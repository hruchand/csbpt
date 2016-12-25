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
typedef struct LPair {
  int d_key;
  int d_tid;       /* tuple ID */
};
struct CSBINODE64*    g_csb_root64;


// csbDelete64:
// Since a table typically grows rather than shrinks, we implement the lazy version of delete.
// Instead of maintaining the minimum occupancy, we simply locate the key on the leaves and delete it.
// return 1 if the entry is deleted, otherwise return 0.
int csbDelete64(struct CSBINODE64* root,struct LPair del_entry) {
  int l,h,m, i;


  while (!IsLeaf(root)) {


    l=0;
    h=root->d_num-1;
    while (l<=h) {
      m=(l+h)>>1;
      if (del_entry.d_key <= root->d_keyList[m])
	h=m-1;
      else
	l=m+1;
    }

    root=(struct CSBINODE64*) root->d_firstChild+l;
  }

  //now search the leaf

  l=0;
  h=((struct BPLNODE64*)root)->d_num-1;
  while (l<=h) {
    m=(l+h)>>1;
    if (del_entry.d_key <= ((struct BPLNODE64*)root)->d_entry[m].d_key)
      h=m-1;
    else
      l=m+1;
  }


  // by now, d_entry[l-1].d_key < key <= d_entry[l].d_key,
  // l can range from 0 to ((BPLNODE64*)root)->d_num
  do {
    while (l<((struct BPLNODE64*)root)->d_num) {
      if (del_entry.d_key==((struct BPLNODE64*)root)->d_entry[l].d_key) {
	if (del_entry.d_tid == ((struct BPLNODE64*)root)->d_entry[l].d_tid) { //delete this entry
	  for (i=l; i<((struct BPLNODE64*)root)->d_num-1; i++)
	    ((struct BPLNODE64*)root)->d_entry[i]=((struct BPLNODE64*)root)->d_entry[i+1];
	  ((struct BPLNODE64*)root)->d_num--;

	  return 1;
	}
	l++;
      }
      else
	return 0;
    }
    root=(struct CSBINODE64*) ((struct BPLNODE64*)root)->d_next;
    l=0;
  }while (root);

  return 0;
}

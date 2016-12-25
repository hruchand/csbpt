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
int csbSearch64(struct CSBINODE64* root, int key) {
  int l,m,h;


  while (!IsLeaf(root)) {


    l=0;
    h=root->d_num-1;
    while (l<=h) {
      m=(l+h)>>1;
      if (key <= root->d_keyList[m])
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
    if (key <= ((struct BPLNODE64*)root)->d_entry[m].d_key)
      h=m-1;
    else
      l=m+1;
  }

  // by now, d_entry[l-1].d_key < key <= d_entry[l].d_key,
  // l can range from 0 to ((BPLNODE64*)root)->d_num

  if (l<((struct BPLNODE64*)root)->d_num && key==((struct BPLNODE64*)root)->d_entry[l].d_key)
    return ((struct BPLNODE64*)root)->d_entry[l].d_tid;
  else
    return 0;
}


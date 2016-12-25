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






void csbInsert64(struct CSBINODE64* root,struct CSBINODE64* parent, int childIndex,struct LPair new_entry, int* new_key, void** new_child) {
  int l,h,m,i,j;



  if (IsLeaf(root)) {    // This is a leaf node


    l=0;
    h=((struct BPLNODE64*)root)->d_num-1;
    while (l<=h) {
      m=(l+h)>>1;
      if (new_entry.d_key <= ((struct BPLNODE64*)root)->d_entry[m].d_key)
	h=m-1;
      else
	l=m+1;
    }

    // by now, d_entry[l-1].d_key < entry.key <= d_entry[l].d_key,
    // l can range from 0 to ((BPLNODE64*)root)->d_num
    // insert entry at the lth position, move everything from l to the right.

    if (((struct BPLNODE64*)root)->d_num < 6) {   //we still have enough space in this leaf node.
      for (i=((struct BPLNODE64*)root)->d_num; i>l; i--)
	((struct BPLNODE64*)root)->d_entry[i]=((struct BPLNODE64*)root)->d_entry[i-1];
      ((struct BPLNODE64*)root)->d_entry[l]=new_entry;
      ((struct BPLNODE64*)root)->d_num++;
      *new_child=0;
    }
    else { // we have to split this leaf node
    	struct BPLNODE64 *new_lnode, *old_lnode;
    	struct BPLNODE64 *new_group, *old_group;

      old_group=(struct BPLNODE64*) parent->d_firstChild;

      if (parent->d_num < 14) { // we don't have to split the parent
	// This bug took me a day. Originally, a node group has parent->d_num+1 nodes.
	// Now we need to allocate space for parent->d_num+2 nodes.



	for (i=parent->d_num+1; i>childIndex+1; i--)
	  old_group[i]=old_group[i-1];
	if (childIndex+1==parent->d_num+1)
	  old_group[parent->d_num+1].d_next=old_group[parent->d_num].d_next;
	for (i=parent->d_num+1; i>childIndex; i--) {
	  old_group[i].d_prev=old_group+i-1;
	  old_group[i-1].d_next=old_group+i;
	}
	if (old_group[parent->d_num+1].d_next)
	  (old_group[parent->d_num+1].d_next)->d_prev=old_group+parent->d_num+1;

	old_lnode=old_group+childIndex;
	new_lnode=old_group+childIndex+1;
	new_group=old_group;

	new_group=(struct BPLNODE64*) mynew(sizeof(struct BPLNODE64)*(parent->d_num+2));
	for (i=0; i<=childIndex; i++)
	  new_group[i]=old_group[i];
	for (i=childIndex+2; i<=parent->d_num+1; i++)
	  new_group[i]=old_group[i-1];
	new_group[0].d_prev=old_group[0].d_prev;
	for (i=1; i<=parent->d_num+1; i++) {
	  new_group[i].d_prev=new_group+i-1;
	  new_group[i-1].d_next=new_group+i;
	}
	new_group[parent->d_num+1].d_next=old_group[parent->d_num].d_next;
	if (new_group[parent->d_num+1].d_next)
	  (new_group[parent->d_num+1].d_next)->d_prev=new_group+parent->d_num+1;
	if (new_group[0].d_prev)
	  (new_group[0].d_prev)->d_next=new_group;
	old_lnode=new_group+childIndex;
	new_lnode=new_group+childIndex+1;

	mydelete(old_group, sizeof(struct BPLNODE64)*parent->d_num);

      }
      else { // we also have to split parent. We have 15+1 nodes, put 8 in each node group.


	new_group=(struct BPLNODE64*) mynew(sizeof(struct BPLNODE64)*(14+1));

	if (childIndex >= (14>>1)) { // the new node (childIndex+1) belongs to new group
	  for (i=(14>>1), j=14; i>=0; i--)
	    if (i != childIndex-(14>>1)) {
	      new_group[i]=old_group[j];
	      j--;
	    }
	  if (childIndex==14)    //the new node is the last one
	    new_group[(14>>1)].d_next=old_group[14].d_next;
	}
	else { //the new node belongs to the old group
	  for (i=(14>>1); i>=0; i--)
	    new_group[i]=old_group[i+(14>>1)];
	  for (i=(14>>1); i>childIndex+1; i--)
	    old_group[i]=old_group[i-1];
	  //old_group[childIndex+1].d_prev=old_group+childIndex;
	  //old_group[childIndex+1].d_next=old_group+childIndex+2;
	}
	new_group[0].d_prev=old_group+(14>>1);
	for (i=1; i<=(14>>1); i++) {
	  new_group[i].d_prev=new_group+i-1;
	  new_group[i-1].d_next=new_group+i;
	  old_group[i].d_prev=old_group+i-1;
	  old_group[i-1].d_next=old_group+i;
	}
	new_group[(14>>1)].d_next=old_group[14].d_next;
	old_group[(14>>1)].d_next=new_group;
	if (new_group[(14>>1)].d_next)
	  (new_group[(14>>1)].d_next)->d_prev=new_group+(14>>1);
	new_lnode=(childIndex+1)>=((14>>1)+1)?
	  new_group+childIndex-(14>>1):old_group+childIndex+1;
	old_lnode=(childIndex)>=((14>>1)+1)?
	  new_group+childIndex-(14>>1)-1:old_group+childIndex;

	mydelete(old_group+(14>>1)+1, sizeof(struct BPLNODE64)*(14>>1));


      }
      if (l > (6>>1)) { //entry should be put in the new node
	for (i=(6>>1)-1, j=6-1; i>=0; i--) {
	  if (i == l-(6>>1)-1) {
	    new_lnode->d_entry[i]=new_entry;
	  }
	  else {
	    new_lnode->d_entry[i]=old_lnode->d_entry[j];
	    j--;
	  }
	}
	//for (i=0; i<(6>>1)+1; i++)
	//  old_lnode->d_entry[i]=((BPLNODE64*)root)->d_entry[i];
      }
      else { //entry should be put in the original node
	for (i=(6>>1)-1; i>=0; i--)
	  new_lnode->d_entry[i]=old_lnode->d_entry[i+(6>>1)];
	for (i=(6>>1); i>l; i--)
	  old_lnode->d_entry[i]=old_lnode->d_entry[i-1];
	old_lnode->d_entry[l]=new_entry;
	//for (i=l-1; i>=0; i--)
	//  old_lnode->d_entry[i]=((BPLNODE64*)root)->d_entry[i];
      }
      new_lnode->d_num=(6>>1);
      new_lnode->d_flag=0;
      old_lnode->d_num=(6>>1)+1;
      *new_key=old_lnode->d_entry[(6>>1)].d_key;
      *new_child=new_group;

    }
  }
  else {  //this is an internal node


    l=0;
    h=root->d_num-1;
    while (l<=h) {
      m=(l+h)>>1;
      if (new_entry.d_key <= root->d_keyList[m])
	h=m-1;
      else
	l=m+1;
    }


    csbInsert64(((struct CSBINODE64*)root->d_firstChild)+l, root, l, new_entry, new_key, new_child);
    if (*new_child) {
      if (root->d_num<14) { // insert the key right here, no further split
	for (i=root->d_num; i>l; i--)
	  root->d_keyList[i]=root->d_keyList[i-1];
	root->d_keyList[i]=*new_key;
	root->d_firstChild=*new_child;  // *new_child represents the pointer to the new node group
	root->d_num++;
	*new_child=0;
      }
      else {	// we have to split again
    	  struct CSBINODE64 *new_node, *old_node, *new_group;

	if (parent==0) { // now, we need to create a new root
	  g_csb_root64=(struct CSBINODE64*) mynew(sizeof(struct CSBINODE64));

	  new_group=(struct CSBINODE64*) mynew(sizeof(struct CSBINODE64)*(14+1));


	  new_group[0]=*root;
	  old_node=new_group;
	  new_node=new_group+1;
	  g_csb_root64->d_num=1;
	  g_csb_root64->d_firstChild=new_group;
	  // g_csb_root64->d_keyList[0] to be filled later.
	  mydelete(root, sizeof(struct CSBINODE64)*1);
	}
	else {  // there is a parent
		struct CSBINODE64 *old_group;

	  old_group=(struct CSBINODE64*) parent->d_firstChild;
	  if (parent->d_num < 14) { // no need to split the parent
	    // same here, now the new node group has parent->d_num+2 nodes.


	    for (i=parent->d_num+1; i>childIndex+1; i--)
	      old_group[i]=old_group[i-1];
	    old_node=old_group+childIndex;
	    new_node=old_group+childIndex+1;
	    new_group=old_group;

	    new_group=(struct CSBINODE64*) mynew(sizeof(struct CSBINODE64)*(parent->d_num+2));
	    for (i=0; i<=childIndex; i++)
	      new_group[i]=old_group[i];
	    for (i=childIndex+2; i<=parent->d_num+1; i++)
	      new_group[i]=old_group[i-1];
	    old_node=new_group+childIndex;
	    new_node=new_group+childIndex+1;

	    mydelete(old_group, sizeof(struct CSBINODE64)*parent->d_num);

	  }
	  else { // we also have to split parent. We have 15+1 nodes, put 8 nodes in each group.


	    new_group=(struct CSBINODE64*) mynew(sizeof(struct CSBINODE64)*(14+1));

	    if (childIndex >= (14>>1)) { // the new node belongs to new group
	      for (i=(14>>1), j=14; i>=0; i--)
		if (i != childIndex-(14>>1)) {
		  new_group[i]=old_group[j];
		  j--;
		}
	    }
	    else { //the new node belongs to the old group
	      for (i=(14>>1); i>=0; i--)
		new_group[i]=old_group[i+(14>>1)];
	      for (i=(14>>1); i>childIndex+1; i--)
		old_group[i]=old_group[i-1];
	    }
	    new_node=(childIndex+1)>=((14>>1)+1)?
	      new_group+childIndex-(14>>1):old_group+childIndex+1;
	    old_node=(childIndex)>=((14>>1)+1)?
	      new_group+childIndex-(14>>1)-1:old_group+childIndex;

	    mydelete(old_group+(14>>1)+1, sizeof(struct BPLNODE64)*(14>>1));

	  }
	}
	// we have 14+1 keys, put the first 8 in old_node and the remaining 7 in new_node.
	// the largest key in old_node is then promoted to the parent.
	if (l > (14>>1)) {     // new_key to be inserted in the new_node
	  for (i=(14>>1)-1, j=14-1; i>=0; i--) {
	    if (i == l-(14>>1)-1)
	      new_node->d_keyList[i]=*new_key;
	    else {
	      new_node->d_keyList[i]=old_node->d_keyList[j];
	      j--;
	    }
	  }
	  //for (i=0; i<(14>>1)+1; i++)
	  //  old_node->d_keyList[i]=root->d_keyList[i];
 	}
	else {    // new_key to be inserted in the old_node
	  for (i=(14>>1)-1; i>=0; i--)
	    new_node->d_keyList[i]=old_node->d_keyList[i+(14>>1)];
	  for (i=(14>>1); i>l; i--)
	    old_node->d_keyList[i]=old_node->d_keyList[i-1];
	  old_node->d_keyList[l]=*new_key;
	  //for (i=l-1; i>=0; i--)
	  //  old_node->d_keyList[i]=root->d_keyList[i];
	}
	new_node->d_num=(14>>1);
	new_node->d_firstChild=*new_child;
	old_node->d_num=(14>>1);
	//old_node->d_firstChild=root->d_firstChild;



	if (parent)
	  *new_key=old_node->d_keyList[14>>1];
	else {
	  g_csb_root64->d_keyList[0]=old_node->d_keyList[14>>1];

	}
	*new_child=new_group;
      }
    }
  }
}

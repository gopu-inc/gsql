# gsql/gsql/btree.py
"""
B+Tree implementation in pure Python
"""

class BPlusTreeNode:
    """Node for B+Tree"""
    
    def __init__(self, is_leaf=False, order=3):
        self.is_leaf = is_leaf
        self.order = order
        self.keys = []
        self.values = [] if is_leaf else []  # For leaves: list of lists of row IDs
        self.children = [] if not is_leaf else None
        self.next = None  # For leaf nodes (linked list)
    
    def __repr__(self):
        return f"BPlusTreeNode(leaf={self.is_leaf}, keys={self.keys})"
    
    def is_full(self):
        """Check if node is full"""
        return len(self.keys) >= self.order
    
    def is_underflow(self):
        """Check if node has too few keys"""
        return len(self.keys) < (self.order // 2)


class BPlusTree:
    """B+Tree index for fast lookups"""
    
    def __init__(self, order=3):
        self.order = order
        self.root = BPlusTreeNode(is_leaf=True, order=order)
        self.height = 1
    
    def insert(self, key, value):
        """Insert key-value pair"""
        leaf = self._find_leaf(self.root, key)
        self._insert_into_leaf(leaf, key, value)
        
        if leaf.is_full():
            self._split_leaf(leaf)
    
    def search(self, key):
        """Search for key"""
        leaf = self._find_leaf(self.root, key)
        
        for i, k in enumerate(leaf.keys):
            if k == key:
                return leaf.values[i]
        return []
    
    def search_range(self, start_key, end_key):
        """Search for keys in range"""
        results = []
        leaf = self._find_leaf(self.root, start_key)
        
        while leaf:
            for i, key in enumerate(leaf.keys):
                if start_key <= key <= end_key:
                    results.extend(leaf.values[i])
                elif key > end_key:
                    return results
            leaf = leaf.next
        
        return results
    
    def _find_leaf(self, node, key):
        """Find leaf node for key"""
        while not node.is_leaf:
            # Find child to follow
            idx = 0
            while idx < len(node.keys) and key >= node.keys[idx]:
                idx += 1
            node = node.children[idx]
        return node
    
    def _insert_into_leaf(self, leaf, key, value):
        """Insert key-value into leaf node"""
        # Find position to insert
        pos = 0
        while pos < len(leaf.keys) and leaf.keys[pos] < key:
            pos += 1
        
        if pos < len(leaf.keys) and leaf.keys[pos] == key:
            # Key exists, append value
            leaf.values[pos].append(value)
        else:
            # Insert new key
            leaf.keys.insert(pos, key)
            leaf.values.insert(pos, [value])
    
    def _split_leaf(self, leaf):
        """Split leaf node"""
        mid = len(leaf.keys) // 2
        
        # Create new leaf
        new_leaf = BPlusTreeNode(is_leaf=True, order=self.order)
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]
        
        # Update original leaf
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]
        
        # Link leaves
        new_leaf.next = leaf.next
        leaf.next = new_leaf
        
        # Promote middle key to parent
        self._insert_into_parent(leaf, new_leaf.keys[0], new_leaf)
    
    def _insert_into_parent(self, left, key, right):
        """Insert key into parent after split"""
        parent = self._find_parent(self.root, left)
        
        if parent is None:
            # Create new root
            new_root = BPlusTreeNode(is_leaf=False, order=self.order)
            new_root.keys = [key]
            new_root.children = [left, right]
            self.root = new_root
            self.height += 1
            return
        
        # Find position to insert
        pos = 0
        while pos < len(parent.keys) and parent.keys[pos] < key:
            pos += 1
        
        # Insert into parent
        parent.keys.insert(pos, key)
        parent.children.insert(pos + 1, right)
        
        if parent.is_full():
            self._split_internal(parent)
    
    def _split_internal(self, node):
        """Split internal node"""
        mid = len(node.keys) // 2
        middle_key = node.keys[mid]
        
        # Create new internal node
        new_node = BPlusTreeNode(is_leaf=False, order=self.order)
        new_node.keys = node.keys[mid + 1:]
        new_node.children = node.children[mid + 1:]
        
        # Update original node
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]
        
        # Insert into parent
        self._insert_into_parent(node, middle_key, new_node)
    
    def _find_parent(self, current, child):
        """Find parent of child node"""
        if current.is_leaf or (current.children and child in current.children):
            return None if current == self.root else current
        
        for i, node in enumerate(current.children):
            parent = self._find_parent(node, child)
            if parent:
                return parent
        return None
    
    def display(self, node=None, level=0):
        """Display tree structure (for debugging)"""
        if node is None:
            node = self.root
        
        indent = "  " * level
        if node.is_leaf:
            print(f"{indent}Leaf: {node.keys}")
        else:
            print(f"{indent}Node: {node.keys}")
            for child in node.children:
                self.display(child, level + 1)

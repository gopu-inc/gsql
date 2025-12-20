# gsql/index.py
from typing import List, Any, Optional
import bisect

class BPlusTreeNode:
    """Nœud d'un B+Tree"""
    
    def __init__(self, is_leaf: bool = False):
        self.is_leaf = is_leaf
        self.keys = []
        self.children = [] if not is_leaf else []
        self.next = None  # Pour les feuilles
        self.values = [] if is_leaf else []  # Pour les feuilles: liste de listes d'IDs
    
    def __repr__(self):
        return f"BPlusTreeNode(leaf={self.is_leaf}, keys={self.keys})"


class BPlusTreeIndex:
    """Index B+Tree pour recherches rapides"""
    
    def __init__(self, order: int = 3):
        self.order = order  # Ordre de l'arbre (max enfants)
        self.root = BPlusTreeNode(is_leaf=True)
        self.height = 1
    
    def insert(self, key: Any, value: int) -> None:
        """Insérer une clé avec sa valeur (ID de ligne)"""
        node = self._find_leaf(self.root, key)
        
        # Insérer dans la feuille
        pos = bisect.bisect_left(node.keys, key)
        
        if pos < len(node.keys) and node.keys[pos] == key:
            # Clé existe déjà, ajouter l'ID
            node.values[pos].append(value)
        else:
            # Nouvelle clé
            node.keys.insert(pos, key)
            node.values.insert(pos, [value])
            
            # Vérifier si la feuille doit être split
            if len(node.keys) > self.order:
                self._split_leaf(node)
    
    def search(self, key: Any) -> List[int]:
        """Rechercher tous les IDs pour une clé"""
        leaf = self._find_leaf(self.root, key)
        pos = bisect.bisect_left(leaf.keys, key)
        
        if pos < len(leaf.keys) and leaf.keys[pos] == key:
            return leaf.values[pos]
        return []
    
    def range_search(self, start: Any, end: Any) -> List[int]:
        """Rechercher toutes les clés dans un intervalle"""
        results = []
        leaf = self._find_leaf(self.root, start)
        
        while leaf:
            for i, key in enumerate(leaf.keys):
                if start <= key <= end:
                    results.extend(leaf.values[i])
                elif key > end:
                    return results
            
            leaf = leaf.next
        
        return results
    
    def remove_by_id(self, row_id: int) -> None:
        """Supprimer un ID de ligne de toutes les clés"""
        self._remove_id_from_node(self.root, row_id)
    
    def _find_leaf(self, node: BPlusTreeNode, key: Any) -> BPlusTreeNode:
        """Trouver la feuille appropriée pour une clé"""
        while not node.is_leaf:
            pos = bisect.bisect_right(node.keys, key)
            node = node.children[pos]
        return node
    
    def _split_leaf(self, node: BPlusTreeNode) -> None:
        """Diviser un nœud feuille"""
        mid = len(node.keys) // 2
        
        # Créer nouvelle feuille
        new_leaf = BPlusTreeNode(is_leaf=True)
        new_leaf.keys = node.keys[mid:]
        new_leaf.values = node.values[mid:]
        
        # Mettre à jour l'ancienne feuille
        node.keys = node.keys[:mid]
        node.values = node.values[:mid]
        
        # Mettre à jour les liens
        new_leaf.next = node.next
        node.next = new_leaf
        
        # Insérer la clé médiane dans le parent
        self._insert_into_parent(node, new_leaf.keys[0], new_leaf)
    
    def _insert_into_parent(self, left: BPlusTreeNode, key: Any, 
                           right: BPlusTreeNode) -> None:
        """Insérer une clé dans le parent après un split"""
        parent = self._find_parent(self.root, left)
        
        if parent is None:
            # Créer une nouvelle racine
            new_root = BPlusTreeNode(is_leaf=False)
            new_root.keys = [key]
            new_root.children = [left, right]
            self.root = new_root
            self.height += 1
            return
        
        # Insérer dans le parent
        pos = bisect.bisect_left(parent.keys, key)
        parent.keys.insert(pos, key)
        parent.children.insert(pos + 1, right)
        
        # Vérifier si le parent doit être split
        if len(parent.keys) > self.order:
            self._split_internal(parent)
    
    def _split_internal(self, node: BPlusTreeNode) -> None:
        """Diviser un nœud interne"""
        mid = len(node.keys) // 2
        median_key = node.keys[mid]
        
        # Créer nouveau nœud interne
        new_node = BPlusTreeNode(is_leaf=False)
        new_node.keys = node.keys[mid + 1:]
        new_node.children = node.children[mid + 1:]
        
        # Mettre à jour l'ancien nœud
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]
        
        # Insérer dans le parent
        self._insert_into_parent(node, median_key, new_node)


class HashIndex:
    """Index de hachage pour recherches exactes rapides"""
    
    def __init__(self, size: int = 1000):
        self.size = size
        self.table = [{} for _ in range(size)]
    
    def _hash(self, key: Any) -> int:
        """Fonction de hachage"""
        return hash(str(key)) % self.size
    
    def insert(self, key: Any, value: int) -> None:
        """Insérer une clé-valeur"""
        idx = self._hash(key)
        if key not in self.table[idx]:
            self.table[idx][key] = []
        self.table[idx][key].append(value)
    
    def search(self, key: Any) -> List[int]:
        """Rechercher une clé"""
        idx = self._hash(key)
        return self.table[idx].get(key, [])

<?php
namespace App\Libraries;

use CodeIgniter\Database\ConnectionInterface;
use RuntimeException;

/**
 * MPTT Library for CodeIgniter 4
 *
 * Implements basic Modified Preorder Tree Traversal (MPTT) operations:
 *  - insertRoot()
 *  - insertChild()
 *  - moveSubtree()
 *  - deleteNode() (optionally delete subtree)
 *  - getTree(), getDescendants(), getAncestors(), getChildren()
 *  - rebuildTree() to rebuild lft/rgt values from parent references
 *
 * Usage example:
 *  $mptt = new \App\Libraries\Mptt();
 *  $rootId = $mptt->insertRoot(['name' => 'Root']);
 *  $child1 = $mptt->insertChild($rootId, ['name' => 'Child 1']);
 *  $child2 = $mptt->insertChild($rootId, ['name' => 'Child 2']);
 *  $mptt->moveSubtree($child1, $child2);
 *
 * Table schema (example migration):
 *
 * CREATE TABLE `categories` (
 *   `id` int(11) NOT NULL AUTO_INCREMENT,
 *   `parent_id` int(11) DEFAULT NULL,
 *   `name` varchar(255) NOT NULL,
 *   `lft` int(11) NOT NULL DEFAULT 0,
 *   `rgt` int(11) NOT NULL DEFAULT 0,
 *   `depth` int(11) NOT NULL DEFAULT 0,
 *   PRIMARY KEY (`id`),
 *   KEY (`lft`),
 *   KEY (`rgt`),
 *   KEY (`parent_id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
 *
 * Notes:
 * - This library uses the database query builder and transactions.
 * - It expects a table with columns: id, parent_id, lft, rgt, depth and arbitrary payload columns.
 */
class Mptt
{
    /** @var ConnectionInterface */
    protected $db;

    /** @var string */
    protected $table = 'categories';

    public function __construct(ConnectionInterface $db = null, string $table = null)
    {
        $this->db = $db ?? db_connect();
        if ($table) {
            $this->table = $table;
        }
    }

    /**
     * Insert a root node (appended to the right-most position)
     * Returns inserted id.
     */
    public function insertRoot(array $data)
    {
        $builder = $this->db->table($this->table);
        $this->db->transStart();

        // Find current max rgt
        $r = $builder->select('MAX(rgt) AS maxr')->get()->getRow();
        $maxr = $r && $r->maxr ? (int)$r->maxr : 0;
        $lft = $maxr + 1;
        $rgt = $maxr + 2;
        $depth = 0;

        $payload = $data + [
            'parent_id' => null,
            'lft' => $lft,
            'rgt' => $rgt,
            'depth' => $depth,
        ];

        $builder->insert($payload);
        $id = $this->db->insertID();

        $this->db->transComplete();
        if ($this->db->transStatus() === false) {
            throw new RuntimeException('Failed to insert root node');
        }

        return (int)$id;
    }

    /**
     * Insert a node as the last child of $parentId
     * Returns inserted id.
     */
    public function insertChild(int $parentId, array $data)
    {
        $builder = $this->db->table($this->table);
        $this->db->transStart();

        $parent = $builder->where('id', $parentId)->get()->getRow();
        if (!$parent) {
            $this->db->transComplete();
            throw new RuntimeException('Parent not found: ' . $parentId);
        }

        $insertAt = (int)$parent->rgt; // new node lft will be parent.rgt
        // Make room for new node: increment lft and rgt values greater or equal to insertAt
        $this->db->query("UPDATE `{$this->table}` SET rgt = rgt + 2 WHERE rgt >= ?", [$insertAt]);
        $this->db->query("UPDATE `{$this->table}` SET lft = lft + 2 WHERE lft >= ?", [$insertAt]);

        $lft = $insertAt;
        $rgt = $insertAt + 1;
        $depth = (int)$parent->depth + 1;

        $payload = $data + [
            'parent_id' => $parentId,
            'lft' => $lft,
            'rgt' => $rgt,
            'depth' => $depth,
        ];

        $builder->insert($payload);
        $id = $this->db->insertID();

        $this->db->transComplete();
        if ($this->db->transStatus() === false) {
            throw new RuntimeException('Failed to insert child node');
        }

        return (int)$id;
    }

    /**
     * Move a subtree rooted at $nodeId to become the last child of $newParentId.
     */
    public function moveSubtree(int $nodeId, int $newParentId)
    {
        if ($nodeId === $newParentId) {
            throw new RuntimeException('Cannot move node under itself');
        }

        $builder = $this->db->table($this->table);
        $this->db->transStart();

        $node = $builder->where('id', $nodeId)->get()->getRow();
        $newParent = $builder->where('id', $newParentId)->get()->getRow();

        if (!$node || !$newParent) {
            $this->db->transComplete();
            throw new RuntimeException('Node or new parent not found');
        }

        // Prevent moving into own subtree
        if ($newParent->lft >= $node->lft && $newParent->rgt <= $node->rgt) {
            $this->db->transComplete();
            throw new RuntimeException('Cannot move a node inside its own subtree');
        }

        $nodeLft = (int)$node->lft;
        $nodeRgt = (int)$node->rgt;
        $nodeWidth = $nodeRgt - $nodeLft + 1;

        $target = (int)$newParent->rgt; // we will insert at this position (before rgt)

        // Step 1: make room at target
        $this->db->query("UPDATE `{$this->table}` SET rgt = rgt + ? WHERE rgt >= ?", [$nodeWidth, $target]);
        $this->db->query("UPDATE `{$this->table}` SET lft = lft + ? WHERE lft >= ?", [$nodeWidth, $target]);

        // If the insertion point is after the node's original position, the node's lft/rgt have shifted by +nodeWidth
        if ($nodeLft >= $target) {
            $nodeLft += $nodeWidth;
            $nodeRgt += $nodeWidth;
        }

        // Step 2: move subtree by temporarily setting negative values to avoid collisions
        $offset = $target - $nodeLft; // how much to shift values
        // Use temporary negation to mark moved nodes
        $this->db->query("UPDATE `{$this->table}` SET lft = -lft, rgt = -rgt WHERE lft >= ? AND rgt <= ?", [$nodeLft, $nodeRgt]);

        // Step 3: close gap left by the subtree
        $this->db->query("UPDATE `{$this->table}` SET lft = lft - ? WHERE lft > ?", [$nodeWidth, $nodeRgt]);
        $this->db->query("UPDATE `{$this->table}` SET rgt = rgt - ? WHERE rgt > ?", [$nodeWidth, $nodeRgt]);

        // Step 4: bring moved nodes into correct position (negated values)
        $this->db->query("UPDATE `{$this->table}` SET lft = -lft + ?, rgt = -rgt + ? WHERE lft < 0", [$offset, $offset]);

        // Step 5: update parent_id and depth for moved subtree
        $depthDiff = $newParent->depth + 1 - $node->depth;
        // update parent_id for root of moved subtree
        $this->db->query("UPDATE `{$this->table}` SET parent_id = ? WHERE id = ?", [$newParentId, $nodeId]);
        // update depths for all nodes in moved subtree
        $this->db->query("UPDATE `{$this->table}` SET depth = depth + ? WHERE lft >= ? AND rgt <= ?", [$depthDiff, $target, $target + $nodeWidth - 1]);

        $this->db->transComplete();
        if ($this->db->transStatus() === false) {
            throw new RuntimeException('Failed moving subtree');
        }

        return true;
    }

    /**
     * Delete a node. If $deleteSubtree is true then delete the entire subtree.
     * If false, this implementation will refuse to delete nodes with children to keep logic simple.
     */
    public function deleteNode(int $nodeId, bool $deleteSubtree = true)
    {
        $builder = $this->db->table($this->table);
        $this->db->transStart();

        $node = $builder->where('id', $nodeId)->get()->getRow();
        if (!$node) {
            $this->db->transComplete();
            throw new RuntimeException('Node not found');
        }

        $l = (int)$node->lft;
        $r = (int)$node->rgt;
        $width = $r - $l + 1;

        if ($deleteSubtree) {
            // delete nodes in the range
            $builder->where('lft >=', $l)->where('rgt <=', $r)->delete();

            // close gap
            $this->db->query("UPDATE `{$this->table}` SET lft = lft - ? WHERE lft > ?", [$width, $r]);
            $this->db->query("UPDATE `{$this->table}` SET rgt = rgt - ? WHERE rgt > ?", [$width, $r]);

            $this->db->transComplete();
            if ($this->db->transStatus() === false) {
                throw new RuntimeException('Failed to delete subtree');
            }

            return true;
        }

        // If not deleting subtree, ensure no children exist (simpler behavior)
        $children = $builder->where('parent_id', $nodeId)->countAllResults();
        if ($children > 0) {
            $this->db->transComplete();
            throw new RuntimeException('Node has children; set $deleteSubtree = true to remove subtree');
        }

        // delete single node
        $builder->where('id', $nodeId)->delete();
        // close gap width=2
        $this->db->query("UPDATE `{$this->table}` SET lft = lft - 2 WHERE lft > ?", [$r]);
        $this->db->query("UPDATE `{$this->table}` SET rgt = rgt - 2 WHERE rgt > ?", [$r]);

        $this->db->transComplete();
        if ($this->db->transStatus() === false) {
            throw new RuntimeException('Failed to delete node');
        }

        return true;
    }

    /**
     * Get full tree ordered by lft (preorder)
     */
    public function getTree()
    {
        $builder = $this->db->table($this->table);
        return $builder->orderBy('lft', 'ASC')->get()->getResultArray();
    }

    /**
     * Get descendants of a node (optionally include the node itself)
     */
    public function getDescendants(int $nodeId, bool $includeSelf = false)
    {
        $builder = $this->db->table($this->table);
        $node = $builder->where('id', $nodeId)->get()->getRow();
        if (!$node) return [];
        if ($includeSelf) {
            $builder->where('lft >=', $node->lft)->where('rgt <=', $node->rgt);
        } else {
            $builder->where('lft >', $node->lft)->where('rgt <', $node->rgt);
        }
        return $builder->orderBy('lft', 'ASC')->get()->getResultArray();
    }

    /**
     * Get ancestors of a node (root first)
     */
    public function getAncestors(int $nodeId, bool $includeSelf = false)
    {
        $builder = $this->db->table($this->table);
        $node = $builder->where('id', $nodeId)->get()->getRow();
        if (!$node) return [];
        if ($includeSelf) {
            $builder->where('lft <=', $node->lft)->where('rgt >=', $node->rgt);
        } else {
            $builder->where('lft <', $node->lft)->where('rgt >', $node->rgt);
        }
        return $builder->orderBy('lft', 'ASC')->get()->getResultArray();
    }

    /**
     * Get direct children of a node
     */
    public function getChildren(int $nodeId)
    {
        $builder = $this->db->table($this->table);
        return $builder->where('parent_id', $nodeId)->orderBy('lft', 'ASC')->get()->getResultArray();
    }

    /**
     * Rebuild lft/rgt/depth values from parent_id references.
     * Useful if you have inconsistent data and need to reindex the tree.
     */
    public function rebuildTree()
    {
        $this->db->transStart();
        // fetch all rows keyed by id
        $rows = $this->db->table($this->table)->orderBy('id', 'ASC')->get()->getResultArray();
        $map = [];
        foreach ($rows as $r) {
            $map[$r['id']] = $r;
        }

        // build adjacency list
        $children = [];
        foreach ($map as $id => $r) {
            $pid = $r['parent_id'] ?? null;
            if ($pid === null) $pid = 0;
            $children[$pid][] = $id;
        }

        $counter = 1;
        $updateRows = [];

        $recurse = function ($parentId, $depth) use (&$recurse, &$children, &$map, &$counter, &$updateRows) {
            if (!isset($children[$parentId])) return;
            foreach ($children[$parentId] as $id) {
                $l = $counter++;
                // recurse children
                $recurse($id, $depth + 1);
                $r = $counter++;
                $updateRows[] = [
                    'id' => $id,
                    'lft' => $l,
                    'rgt' => $r,
                    'depth' => $depth + 1,
                ];
            }
        };

        // start from true roots (parent 0 / NULL)
        $recurse(0, -1); // root depth will become 0 for top-level nodes

        // apply updates
        foreach ($updateRows as $u) {
            $this->db->table($this->table)->where('id', $u['id'])->update([
                'lft' => $u['lft'],
                'rgt' => $u['rgt'],
                'depth' => $u['depth'],
            ]);
        }

        $this->db->transComplete();
        if ($this->db->transStatus() === false) {
            throw new RuntimeException('Failed to rebuild tree');
        }

        return true;
    }

    /**
     * Helper: fetch a node by id
     */
    public function getNode(int $id)
    {
        return $this->db->table($this->table)->where('id', $id)->get()->getRowArray();
    }
}
import {
  rowKeyByField,
  type DeltaEvent,
  type DeltaOperation,
  type LiveQueryStatusEvent,
  type RuntimeRow,
  type RuntimeRowKey,
  type RuntimeRowKeyFn,
  type SnapshotEvent,
  type SubscriptionEvent,
} from "../protocol/index.ts";

export type VisibleRowsSnapshot<TRow extends RuntimeRow = RuntimeRow> = {
  readonly rows: readonly TRow[];
  readonly totalRows: number;
  readonly version: bigint;
};

export type VisibleRowsStatus<TRow extends RuntimeRow = RuntimeRow> = {
  readonly rows: readonly TRow[];
  readonly totalRows: number;
  readonly status: LiveQueryStatusEvent["status"];
};

export function isCurrentSubscriptionEvent<TRow extends readonly RuntimeRow[]>(
  event: SubscriptionEvent<TRow>,
  requestId: string,
): boolean {
  return event.requestId === requestId;
}

export function applySnapshot<TRow extends readonly RuntimeRow[]>(
  event: SnapshotEvent<TRow>,
): VisibleRowsSnapshot<TRow[number]> {
  return {
    rows: event.rows,
    totalRows: event.meta.totalRows,
    version: BigInt(event.meta.version),
  };
}

export function applyStatus<TRow extends RuntimeRow>(
  rows: readonly TRow[],
  event: LiveQueryStatusEvent,
): VisibleRowsStatus<TRow> {
  return {
    rows,
    totalRows: event.meta.totalRows,
    status: event.status,
  };
}

export function applyDeltaOperations(
  rows: readonly RuntimeRow[],
  event: DeltaEvent<readonly RuntimeRow[]>,
  rowKeyOrIdField: RuntimeRowKeyFn | string = "id",
): readonly RuntimeRow[] {
  const rowKey =
    typeof rowKeyOrIdField === "string"
      ? (row: RuntimeRow) => rowKeyByField(row, rowKeyOrIdField)
      : rowKeyOrIdField;
  if (shouldUseIndexedTree(rows.length, event.ops.length)) {
    return applyDeltaOperationsWithTree(rows, event.ops, rowKey);
  }
  return applyDeltaOperationsSequentially(rows, event.ops, rowKey);
}

function shouldUseIndexedTree(rowCount: number, operationCount: number): boolean {
  return rowCount >= 1_000 && operationCount >= 32;
}

function applyDeltaOperationsSequentially(
  rows: readonly RuntimeRow[],
  operations: readonly DeltaOperation<RuntimeRow>[],
  rowKey: RuntimeRowKeyFn,
): readonly RuntimeRow[] {
  const next = rows.map((row) => ({ ...row }));
  const indexesByKey = indexRows(next, rowKey);
  for (const operation of operations) {
    applyDeltaOperationWithIndex(next, operation, rowKey, indexesByKey);
  }
  return next;
}

function applyDeltaOperationWithIndex(
  rows: RuntimeRow[],
  operation: DeltaOperation<RuntimeRow>,
  rowKey: RuntimeRowKeyFn,
  indexesByKey: Map<RuntimeRowKey, number>,
): void {
  if (operation.type === "remove") {
    const index = indexesByKey.get(operation.key);
    if (index !== undefined) {
      removeAt(rows, index, rowKey, indexesByKey);
    }
    return;
  }

  if (operation.type === "patch") {
    const index = indexesByKey.get(operation.key);
    if (index !== undefined) {
      const previous = rows[index];
      if (previous !== undefined) {
        removeAt(rows, index, rowKey, indexesByKey);
        insertAt(
          rows,
          normalizeIndex(operation.index, rows.length, index),
          { ...previous, ...operation.changes },
          rowKey,
          indexesByKey,
        );
      }
    }
    return;
  }

  const key = operation.key ?? rowKey(operation.row);
  const index = indexesByKey.get(key);
  const fallbackIndex = index ?? rows.length;
  if (index !== undefined) {
    removeAt(rows, index, rowKey, indexesByKey);
  }
  insertAt(
    rows,
    normalizeIndex(operation.index, rows.length, fallbackIndex),
    operation.row,
    rowKey,
    indexesByKey,
  );
}

function removeAt(
  rows: RuntimeRow[],
  index: number,
  rowKey: RuntimeRowKeyFn,
  indexesByKey: Map<RuntimeRowKey, number>,
): void {
  const removed = rows[index];
  if (removed === undefined) {
    return;
  }
  indexesByKey.delete(rowKey(removed));
  rows.splice(index, 1);
  reindexFrom(rows, index, rowKey, indexesByKey);
}

function insertAt(
  rows: RuntimeRow[],
  index: number,
  row: RuntimeRow,
  rowKey: RuntimeRowKeyFn,
  indexesByKey: Map<RuntimeRowKey, number>,
): void {
  rows.splice(index, 0, row);
  reindexFrom(rows, index, rowKey, indexesByKey);
}

function indexRows(
  rows: readonly RuntimeRow[],
  rowKey: RuntimeRowKeyFn,
): Map<RuntimeRowKey, number> {
  const indexesByKey = new Map<RuntimeRowKey, number>();
  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    if (row !== undefined && !indexesByKey.has(rowKey(row))) {
      indexesByKey.set(rowKey(row), index);
    }
  }
  return indexesByKey;
}

function reindexFrom(
  rows: readonly RuntimeRow[],
  start: number,
  rowKey: RuntimeRowKeyFn,
  indexesByKey: Map<RuntimeRowKey, number>,
): void {
  for (let index = start; index < rows.length; index++) {
    const row = rows[index];
    if (row !== undefined) {
      indexesByKey.set(rowKey(row), index);
    }
  }
}

function normalizeIndex(index: number | undefined, length: number, fallback: number): number {
  if (index === undefined || !Number.isFinite(index)) {
    return Math.max(0, Math.min(length, fallback));
  }
  return Math.max(0, Math.min(length, Math.trunc(index)));
}

class DeltaTreeNode {
  left: DeltaTreeNode | undefined;
  right: DeltaTreeNode | undefined;
  parent: DeltaTreeNode | undefined;
  size = 1;
  row: RuntimeRow;
  readonly priority: number;

  constructor(row: RuntimeRow, priority: number) {
    this.row = row;
    this.priority = priority;
  }
}

type DeltaTreeSplit = readonly [left: DeltaTreeNode | undefined, right: DeltaTreeNode | undefined];

type DeltaTreeRemoveResult = {
  readonly root: DeltaTreeNode | undefined;
  readonly removed: DeltaTreeNode | undefined;
};

function applyDeltaOperationsWithTree(
  rows: readonly RuntimeRow[],
  operations: readonly DeltaOperation<RuntimeRow>[],
  rowKey: RuntimeRowKeyFn,
): readonly RuntimeRow[] {
  const index = buildDeltaTree(rows, rowKey);
  let root = index.root;
  let nextPriority = rows.length + 1;
  for (const operation of operations) {
    if (operation.type === "remove") {
      const node = index.nodesByKey.get(operation.key);
      if (node !== undefined) {
        const result = removeTreeNode(root, node);
        root = result.root;
        index.nodesByKey.delete(operation.key);
      }
      continue;
    }

    if (operation.type === "patch") {
      const node = index.nodesByKey.get(operation.key);
      if (node !== undefined) {
        const currentRank = treeRank(node);
        const result = removeTreeNode(root, node);
        root = result.root;
        index.nodesByKey.delete(operation.key);
        node.row = { ...node.row, ...operation.changes };
        resetTreeNode(node);
        root = insertTreeNode(
          root,
          normalizeIndex(operation.index, treeSize(root), currentRank),
          node,
        );
        index.nodesByKey.set(rowKey(node.row), node);
      }
      continue;
    }

    const key = operation.key ?? rowKey(operation.row);
    const existing = index.nodesByKey.get(key);
    const fallbackIndex = existing === undefined ? treeSize(root) : treeRank(existing);
    if (existing !== undefined) {
      const result = removeTreeNode(root, existing);
      root = result.root;
      index.nodesByKey.delete(key);
    }
    const node =
      existing === undefined
        ? new DeltaTreeNode(operation.row, priorityForIndex(nextPriority++))
        : existing;
    node.row = operation.row;
    resetTreeNode(node);
    root = insertTreeNode(
      root,
      normalizeIndex(operation.index, treeSize(root), fallbackIndex),
      node,
    );
    index.nodesByKey.set(rowKey(operation.row), node);
  }
  return flattenTree(root);
}

function buildDeltaTree(
  rows: readonly RuntimeRow[],
  rowKey: RuntimeRowKeyFn,
): {
  readonly root: DeltaTreeNode | undefined;
  readonly nodesByKey: Map<RuntimeRowKey, DeltaTreeNode>;
} {
  let root: DeltaTreeNode | undefined;
  const nodesByKey = new Map<RuntimeRowKey, DeltaTreeNode>();
  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    if (row !== undefined) {
      const node = new DeltaTreeNode({ ...row }, priorityForIndex(index));
      if (!nodesByKey.has(rowKey(row))) {
        nodesByKey.set(rowKey(row), node);
      }
      root = mergeTrees(root, node);
    }
  }
  setTreeParent(root, undefined);
  return { root, nodesByKey };
}

function removeTreeNode(
  root: DeltaTreeNode | undefined,
  node: DeltaTreeNode,
): DeltaTreeRemoveResult {
  const rank = treeRank(node);
  const [before, fromNode] = splitTree(root, rank);
  const [removed, after] = splitTree(fromNode, 1);
  const nextRoot = mergeTrees(before, after);
  setTreeParent(nextRoot, undefined);
  if (removed !== undefined) {
    resetTreeNode(removed);
  }
  return {
    root: nextRoot,
    removed,
  };
}

function insertTreeNode(
  root: DeltaTreeNode | undefined,
  index: number,
  node: DeltaTreeNode,
): DeltaTreeNode | undefined {
  const [before, after] = splitTree(root, index);
  const nextRoot = mergeTrees(mergeTrees(before, node), after);
  setTreeParent(nextRoot, undefined);
  return nextRoot;
}

function splitTree(root: DeltaTreeNode | undefined, count: number): DeltaTreeSplit {
  if (root === undefined) {
    return [undefined, undefined];
  }
  if (treeSize(root.left) >= count) {
    const [left, rightLeft] = splitTree(root.left, count);
    setTreeLeft(root, rightLeft);
    setTreeParent(left, undefined);
    setTreeParent(root, undefined);
    return [left, root];
  }
  const [leftRight, right] = splitTree(root.right, count - treeSize(root.left) - 1);
  setTreeRight(root, leftRight);
  setTreeParent(root, undefined);
  setTreeParent(right, undefined);
  return [root, right];
}

function mergeTrees(
  left: DeltaTreeNode | undefined,
  right: DeltaTreeNode | undefined,
): DeltaTreeNode | undefined {
  if (left === undefined) {
    setTreeParent(right, undefined);
    return right;
  }
  if (right === undefined) {
    setTreeParent(left, undefined);
    return left;
  }
  if (left.priority <= right.priority) {
    setTreeRight(left, mergeTrees(left.right, right));
    setTreeParent(left, undefined);
    return left;
  }
  setTreeLeft(right, mergeTrees(left, right.left));
  setTreeParent(right, undefined);
  return right;
}

function treeRank(node: DeltaTreeNode): number {
  let rank = treeSize(node.left);
  let current: DeltaTreeNode | undefined = node;
  while (current !== undefined && current.parent !== undefined) {
    const parent: DeltaTreeNode = current.parent;
    if (parent.right === current) {
      rank += treeSize(parent.left) + 1;
    }
    current = parent;
  }
  return rank;
}

function flattenTree(root: DeltaTreeNode | undefined): readonly RuntimeRow[] {
  const rows: RuntimeRow[] = [];
  const stack: DeltaTreeNode[] = [];
  let current = root;
  while (current !== undefined || stack.length > 0) {
    while (current !== undefined) {
      stack.push(current);
      current = current.left;
    }
    const node = stack.pop();
    if (node !== undefined) {
      rows.push(node.row);
      current = node.right;
    }
  }
  return rows;
}

function resetTreeNode(node: DeltaTreeNode): void {
  node.left = undefined;
  node.right = undefined;
  node.parent = undefined;
  node.size = 1;
}

function setTreeLeft(parent: DeltaTreeNode, child: DeltaTreeNode | undefined): void {
  parent.left = child;
  setTreeParent(child, parent);
  refreshTreeSize(parent);
}

function setTreeRight(parent: DeltaTreeNode, child: DeltaTreeNode | undefined): void {
  parent.right = child;
  setTreeParent(child, parent);
  refreshTreeSize(parent);
}

function setTreeParent(node: DeltaTreeNode | undefined, parent: DeltaTreeNode | undefined): void {
  if (node !== undefined) {
    node.parent = parent;
  }
}

function refreshTreeSize(node: DeltaTreeNode): void {
  node.size = treeSize(node.left) + treeSize(node.right) + 1;
}

function treeSize(node: DeltaTreeNode | undefined): number {
  return node?.size ?? 0;
}

function priorityForIndex(index: number): number {
  let value = Math.imul(index + 1, 2_654_435_761);
  value ^= value >>> 16;
  value = Math.imul(value, 2_246_822_519);
  value ^= value >>> 13;
  return value >>> 0;
}

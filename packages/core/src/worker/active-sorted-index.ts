import type { RuntimeRowKey } from "../protocol/index.ts";

export type ActiveSortedIndexKind = "array" | "blocks";

export type ActiveSortedIndex = {
  readonly kind: ActiveSortedIndexKind;
  readonly size: () => number;
  readonly estimatedSizeBytes: () => number;
  readonly insert: (id: RuntimeRowKey) => void;
  readonly remove: (id: RuntimeRowKey) => void;
  readonly slice: (offset: number, limit: number) => readonly RuntimeRowKey[];
};

export type ActiveSortedIndexOptions = {
  readonly kind: ActiveSortedIndexKind;
  readonly compareIds: (left: RuntimeRowKey, right: RuntimeRowKey) => number;
  readonly blockSize?: number | undefined;
};

export type ActiveSortedIndexByteEstimateOptions = {
  readonly kind?: ActiveSortedIndexKind | undefined;
  readonly blockSize?: number | undefined;
};

const DEFAULT_BLOCK_SIZE = 1024;
const ARRAY_HEADER_BYTES = 24;
const BLOCK_HEADER_BYTES = 32;
const KEY_REFERENCE_BYTES = 16;

export function makeActiveSortedIndex(
  ids: Iterable<RuntimeRowKey>,
  options: ActiveSortedIndexOptions,
): ActiveSortedIndex {
  return options.kind === "blocks"
    ? new BlockSortedIndex(ids, options.compareIds, options.blockSize ?? DEFAULT_BLOCK_SIZE, false)
    : new ArraySortedIndex(ids, options.compareIds, false);
}

export function makeActiveSortedIndexFromSortedIds(
  ids: Iterable<RuntimeRowKey>,
  options: ActiveSortedIndexOptions,
): ActiveSortedIndex {
  return options.kind === "blocks"
    ? new BlockSortedIndex(ids, options.compareIds, options.blockSize ?? DEFAULT_BLOCK_SIZE, true)
    : new ArraySortedIndex(ids, options.compareIds, true);
}

export function estimateActiveSortedIndexBytes(
  rowCount: number,
  options: ActiveSortedIndexByteEstimateOptions = {},
): number {
  const count = Math.max(0, Math.trunc(rowCount));
  if (options.kind === "array") {
    return ARRAY_HEADER_BYTES + count * KEY_REFERENCE_BYTES;
  }
  const blockSize = normalizedBlockSize(options.blockSize ?? DEFAULT_BLOCK_SIZE);
  const blockCount = count === 0 ? 0 : Math.ceil(count / blockSize);
  return ARRAY_HEADER_BYTES + blockCount * BLOCK_HEADER_BYTES + count * KEY_REFERENCE_BYTES;
}

class ArraySortedIndex implements ActiveSortedIndex {
  readonly kind = "array";
  readonly #compareIds: (left: RuntimeRowKey, right: RuntimeRowKey) => number;
  readonly #ids: RuntimeRowKey[];

  constructor(
    ids: Iterable<RuntimeRowKey>,
    compareIds: (left: RuntimeRowKey, right: RuntimeRowKey) => number,
    sorted: boolean,
  ) {
    this.#compareIds = compareIds;
    this.#ids = sorted ? Array.from(ids) : Array.from(ids).sort(compareIds);
  }

  size(): number {
    return this.#ids.length;
  }

  estimatedSizeBytes(): number {
    return estimateActiveSortedIndexBytes(this.#ids.length, { kind: "array" });
  }

  insert(id: RuntimeRowKey): void {
    this.#ids.splice(this.insertionIndex(id), 0, id);
  }

  remove(id: RuntimeRowKey): void {
    const index = this.indexOf(id);
    if (index >= 0) {
      this.#ids.splice(index, 1);
    }
  }

  slice(offset: number, limit: number): readonly RuntimeRowKey[] {
    return this.#ids.slice(offset, offset + limit);
  }

  private insertionIndex(id: RuntimeRowKey): number {
    let low = 0;
    let high = this.#ids.length;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      const compared = this.#compareIds(this.#ids[middle], id);
      if (compared <= 0) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  private indexOf(id: RuntimeRowKey): number {
    let low = 0;
    let high = this.#ids.length - 1;
    while (low <= high) {
      const middle = Math.floor((low + high) / 2);
      const compared = this.#compareIds(this.#ids[middle], id);
      if (compared < 0) {
        low = middle + 1;
      } else if (compared > 0) {
        high = middle - 1;
      } else {
        return this.exactIndexInEqualRange(middle, id);
      }
    }
    return -1;
  }

  private exactIndexInEqualRange(index: number, id: RuntimeRowKey): number {
    for (
      let candidateIndex = index;
      candidateIndex >= 0 && this.#compareIds(this.#ids[candidateIndex], id) === 0;
      candidateIndex--
    ) {
      if (Object.is(this.#ids[candidateIndex], id)) {
        return candidateIndex;
      }
    }
    for (
      let candidateIndex = index + 1;
      candidateIndex < this.#ids.length && this.#compareIds(this.#ids[candidateIndex], id) === 0;
      candidateIndex++
    ) {
      if (Object.is(this.#ids[candidateIndex], id)) {
        return candidateIndex;
      }
    }
    return -1;
  }
}

class BlockSortedIndex implements ActiveSortedIndex {
  readonly kind = "blocks";
  readonly #compareIds: (left: RuntimeRowKey, right: RuntimeRowKey) => number;
  readonly #targetBlockSize: number;
  readonly #minBlockSize: number;
  readonly #maxBlockSize: number;
  readonly #blocks: RuntimeRowKey[][] = [];
  #size = 0;

  constructor(
    ids: Iterable<RuntimeRowKey>,
    compareIds: (left: RuntimeRowKey, right: RuntimeRowKey) => number,
    blockSize: number,
    sortedInput: boolean,
  ) {
    this.#compareIds = compareIds;
    this.#targetBlockSize = normalizedBlockSize(blockSize);
    this.#minBlockSize = Math.floor(this.#targetBlockSize / 2);
    this.#maxBlockSize = this.#targetBlockSize * 2;
    const sorted = sortedInput ? Array.from(ids) : Array.from(ids).sort(compareIds);
    this.#size = sorted.length;
    for (let index = 0; index < sorted.length; index += this.#targetBlockSize) {
      this.#blocks.push(sorted.slice(index, index + this.#targetBlockSize));
    }
  }

  size(): number {
    return this.#size;
  }

  estimatedSizeBytes(): number {
    return estimateActiveSortedIndexBytes(this.#size, {
      kind: "blocks",
      blockSize: this.#targetBlockSize,
    });
  }

  insert(id: RuntimeRowKey): void {
    if (this.#blocks.length === 0) {
      this.#blocks.push([id]);
      this.#size = 1;
      return;
    }
    const blockIndex = this.insertionBlockIndex(id);
    const block = this.#blocks[blockIndex];
    if (block === undefined) {
      this.#blocks.push([id]);
      this.#size++;
      return;
    }
    block.splice(this.insertionIndexInBlock(block, id), 0, id);
    this.#size++;
    this.splitIfNeeded(blockIndex);
  }

  remove(id: RuntimeRowKey): void {
    const location = this.locationOf(id);
    if (location === undefined) {
      return;
    }
    const block = this.#blocks[location.blockIndex];
    if (block === undefined) {
      return;
    }
    block.splice(location.index, 1);
    this.#size--;
    this.compactIfNeeded(location.blockIndex);
  }

  slice(offset: number, limit: number): readonly RuntimeRowKey[] {
    if (limit <= 0 || offset >= this.#size) {
      return [];
    }
    const result: RuntimeRowKey[] = [];
    let skipped = offset;
    for (const block of this.#blocks) {
      if (skipped >= block.length) {
        skipped -= block.length;
        continue;
      }
      const start = skipped;
      const remaining = limit - result.length;
      result.push(...block.slice(start, start + remaining));
      if (result.length >= limit) {
        break;
      }
      skipped = 0;
    }
    return result;
  }

  private insertionBlockIndex(id: RuntimeRowKey): number {
    let low = 0;
    let high = this.#blocks.length;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      const lastId = last(this.#blocks[middle]);
      if (lastId !== undefined && this.#compareIds(lastId, id) <= 0) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return Math.min(low, this.#blocks.length - 1);
  }

  private insertionIndexInBlock(block: readonly RuntimeRowKey[], id: RuntimeRowKey): number {
    let low = 0;
    let high = block.length;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      const compared = this.#compareIds(block[middle], id);
      if (compared <= 0) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  private locationOf(
    id: RuntimeRowKey,
  ): { readonly blockIndex: number; readonly index: number } | undefined {
    let blockIndex = this.firstBlockWithLastAtLeast(id);
    while (blockIndex < this.#blocks.length) {
      const block = this.#blocks[blockIndex];
      const firstId = first(block);
      const lastId = last(block);
      if (firstId === undefined || lastId === undefined) {
        blockIndex++;
        continue;
      }
      if (this.#compareIds(firstId, id) > 0) {
        return undefined;
      }
      if (this.#compareIds(lastId, id) < 0) {
        blockIndex++;
        continue;
      }
      const index = this.exactIndexInBlock(block, id);
      if (index >= 0) {
        return { blockIndex, index };
      }
      blockIndex++;
    }
    return undefined;
  }

  private firstBlockWithLastAtLeast(id: RuntimeRowKey): number {
    let low = 0;
    let high = this.#blocks.length;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      const lastId = last(this.#blocks[middle]);
      if (lastId !== undefined && this.#compareIds(lastId, id) < 0) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  private exactIndexInBlock(block: readonly RuntimeRowKey[], id: RuntimeRowKey): number {
    let low = 0;
    let high = block.length - 1;
    while (low <= high) {
      const middle = Math.floor((low + high) / 2);
      const compared = this.#compareIds(block[middle], id);
      if (compared < 0) {
        low = middle + 1;
      } else if (compared > 0) {
        high = middle - 1;
      } else {
        return this.exactIndexInBlockEqualRange(block, middle, id);
      }
    }
    return -1;
  }

  private exactIndexInBlockEqualRange(
    block: readonly RuntimeRowKey[],
    index: number,
    id: RuntimeRowKey,
  ): number {
    for (
      let candidateIndex = index;
      candidateIndex >= 0 && this.#compareIds(block[candidateIndex], id) === 0;
      candidateIndex--
    ) {
      if (Object.is(block[candidateIndex], id)) {
        return candidateIndex;
      }
    }
    for (
      let candidateIndex = index + 1;
      candidateIndex < block.length && this.#compareIds(block[candidateIndex], id) === 0;
      candidateIndex++
    ) {
      if (Object.is(block[candidateIndex], id)) {
        return candidateIndex;
      }
    }
    return -1;
  }

  private splitIfNeeded(blockIndex: number): void {
    const block = this.#blocks[blockIndex];
    if (block === undefined || block.length <= this.#maxBlockSize) {
      return;
    }
    const middle = Math.floor(block.length / 2);
    const next = block.splice(middle);
    this.#blocks.splice(blockIndex + 1, 0, next);
  }

  private compactIfNeeded(blockIndex: number): void {
    const block = this.#blocks[blockIndex];
    if (block === undefined) {
      return;
    }
    if (block.length === 0) {
      this.#blocks.splice(blockIndex, 1);
      return;
    }
    if (block.length >= this.#minBlockSize) {
      return;
    }
    const next = this.#blocks[blockIndex + 1];
    if (next !== undefined && block.length + next.length <= this.#maxBlockSize) {
      block.push(...next);
      this.#blocks.splice(blockIndex + 1, 1);
      return;
    }
    const previous = this.#blocks[blockIndex - 1];
    if (previous !== undefined && previous.length + block.length <= this.#maxBlockSize) {
      previous.push(...block);
      this.#blocks.splice(blockIndex, 1);
    }
  }
}

function first(values: readonly RuntimeRowKey[] | undefined): RuntimeRowKey | undefined {
  return values?.[0];
}

function last(values: readonly RuntimeRowKey[] | undefined): RuntimeRowKey | undefined {
  return values?.[values.length - 1];
}

function normalizedBlockSize(blockSize: number): number {
  return Math.max(32, Math.trunc(blockSize));
}

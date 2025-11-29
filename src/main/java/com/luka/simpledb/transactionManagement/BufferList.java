package com.luka.simpledb.transactionManagement;

import com.luka.simpledb.bufferManagement.Buffer;
import com.luka.simpledb.bufferManagement.BufferManager;
import com.luka.simpledb.fileManagement.BlockId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/// Manages the list of pinned buffers for one transaction.
public class BufferList {
    private final Map<BlockId, Buffer> buffers = new HashMap<>();
    private final List<BlockId> pins = new ArrayList<>();
    private final BufferManager bufferManager;

    /// Since the buffer list works on the buffer level, it needs
    /// a buffer manager to pin and unpin them.
    public BufferList(BufferManager bufferManager) {
        this.bufferManager = bufferManager;
    }

    /// @return The buffer for the requested block id from the list of
    /// pinned buffers of a transaction.
    public Buffer getBuffer(BlockId blockId) {
        return buffers.get(blockId);
    }

    /// Pins a buffer to the passed block id and puts
    /// the buffer in the pinned buffers of this transaction.
    public void pin(BlockId blockId) {
        Buffer buffer = bufferManager.pin(blockId);
        buffers.put(blockId, buffer);
        pins.add(blockId);
    }

    /// Unpins the block id from its correspondent pinned buffer once.
    /// If the pin counter on that buffer reaches zero, the buffer
    /// is removed from the transaction's buffer list.
    public void unpin(BlockId blockId) {
        Buffer buffer = buffers.get(blockId);
        if (buffer == null) {
            return;
        }
        bufferManager.unpin(buffer);
        pins.remove(blockId);
        if (!pins.contains(blockId)) {
            buffers.remove(blockId);
        }
    }

    /// Unpins all buffers for the transaction.
    public void unpinAll() {
        for (BlockId blockId : pins) {
            Buffer buffer = buffers.get(blockId);
            bufferManager.unpin(buffer);
        }
        buffers.clear();
        pins.clear();
    }
}

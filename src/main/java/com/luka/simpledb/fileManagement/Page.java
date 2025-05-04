package com.luka.simpledb.fileManagement;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    private ByteBuffer byteBuffer;
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    public Page(int blockSize) {
        byteBuffer = ByteBuffer.allocateDirect(blockSize);
    }

    public Page(byte[] b) {
        byteBuffer = ByteBuffer.wrap(b);
    }

    public int getInt(int offset) {
        return byteBuffer.getInt(offset);
    }

    public void setInt(int offset, int integer) {
        byteBuffer.putInt(offset, integer);
    }

    public byte[] getBytes(int offset) {
        byteBuffer.position(offset);
        int length = byteBuffer.getInt();
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);

        return bytes;
    }

    public void setBytes(int offset, byte[] bytes) {
        byteBuffer.position(offset);
        byteBuffer.putInt(bytes.length);
        byteBuffer.put(bytes);
    }

    public String getString(int offset) {
        byte[] strbytes = getBytes(offset);

        return new String(strbytes, CHARSET);
    }

    public void setString(int offset, String string) {
        setBytes(offset, string.getBytes(CHARSET));
    }

    public boolean getBoolean(int offset) {
        return byteBuffer.get(offset) == 1;
    }

    public void setBoolean(int offset, boolean bool) {
        byteBuffer.put(offset, (byte) (bool ? 1 : 0 ));
    }

    public static int maxLength(int strlen) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strlen * (int)bytesPerChar);
    }

    ByteBuffer contents() {
        byteBuffer.position(0);
        return byteBuffer;
    }
}

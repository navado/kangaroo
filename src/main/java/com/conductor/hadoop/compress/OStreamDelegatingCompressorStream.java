package com.conductor.hadoop.compress;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;

/**
 * A {@link org.apache.hadoop.io.compress.CompressorStream} that delegates compression to an internal
 * {@link OutputStream} instead of to a {@link Compressor}.
 * <p/>
 * 2015
 *
 * @author Tyrone Hinderson
 */
public final class OStreamDelegatingCompressorStream extends CompressorStream {

    public OStreamDelegatingCompressorStream(final OutputStream out) throws IOException {
        super(out);
    }

    @Override
    public void write(final int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(final byte[] data, final int offset, final int length) throws IOException {
        out.write(data, offset, length);
    }

    @Override
    public void close() throws IOException {
        flush();
        out.close();
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void finish() throws IOException {
    }

    @Override
    public void resetState() throws IOException {
    }
}

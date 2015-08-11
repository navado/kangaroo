package com.conductor.hadoop.compress;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;

/**
 * A {@link org.apache.hadoop.io.compress.DecompressorStream} that delegates compression to an internal
 * {@link InputStream} rather than to a {@link Decompressor}.
 * <p/>
 * 2015
 *
 * @author Tyrone Hinderson
 */
public final class IStreamDelegatingDecompressorStream extends DecompressorStream {

    public IStreamDelegatingDecompressorStream(final InputStream in) throws IOException {
        super(in);
    }

    @Override
    public int read() throws IOException {
        checkStream();
        return in.read();
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        checkStream();
        return in.read(b, off, len);
    }

    @Override
    public void resetState() throws IOException {
    }
}

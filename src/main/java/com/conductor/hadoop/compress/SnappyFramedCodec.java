package com.conductor.hadoop.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

/**
 * Compression codec for the Snappy Framing Format.
 * <p>
 * This codec does not make use of {@link Compressor} or {@link Decompressor}, because all the actual compression work
 * is handled by {@link SnappyFramedOutputStream} and {@link SnappyFramedInputStream}.
 * </p>
 * 2015
 *
 * @see OStreamDelegatingCompressorStream
 * @see IStreamDelegatingDecompressorStream
 *
 * @author Tyrone Hinderson
 */
public class SnappyFramedCodec implements Configurable, CompressionCodec {
    private static final Log LOG = LogFactory.getLog(SnappyFramedCodec.class);
    public static final String COMPRESSION_BLOCK_SIZE_CONF = "io.compression.codec.snappyframed.blocksize";
    private Configuration conf;

    public SnappyFramedCodec() {
        this(new Configuration());
    }

    public SnappyFramedCodec(final Configuration conf) {
        this.conf = conf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompressionOutputStream createOutputStream(final OutputStream out) throws IOException {
        return createOutputStream(out, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompressionOutputStream createOutputStream(final OutputStream out, final Compressor compressor)
            throws IOException {
        LOG.info("Creating compressor stream");
        return new OStreamDelegatingCompressorStream(new SnappyFramedOutputStream(out, conf.getInt(
                COMPRESSION_BLOCK_SIZE_CONF, SnappyFramedOutputStream.DEFAULT_BLOCK_SIZE),
                SnappyFramedOutputStream.DEFAULT_MIN_COMPRESSION_RATIO));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<? extends Compressor> getCompressorType() {
        LOG.warn("This codec doesn't use a compressor, returning null.");
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Compressor createCompressor() {
        LOG.warn("This codec doesn't use a compressor, returning null.");
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompressionInputStream createInputStream(final InputStream in) throws IOException {
        return createInputStream(in, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompressionInputStream createInputStream(final InputStream in, final Decompressor decompressor)
            throws IOException {
        LOG.debug("Creating decompressor stream");
        return new IStreamDelegatingDecompressorStream(new SnappyFramedInputStream(in));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<? extends Decompressor> getDecompressorType() {
        LOG.warn("This codec doesn't use a decompressor, returning null.");
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Decompressor createDecompressor() {
        LOG.warn("This codec doesn't use a decompressor, returning null.");
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDefaultExtension() {
        return ".sz";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration getConf() {
        return conf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConf(final Configuration conf) {
        this.conf = conf;
    }
}

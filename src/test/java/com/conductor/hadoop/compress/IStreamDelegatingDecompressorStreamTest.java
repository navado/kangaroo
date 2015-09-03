package com.conductor.hadoop.compress;

import java.io.InputStream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for the {@link IStreamDelegatingDecompressorStream}
 * <p/>
 * 2014
 *
 * @author Tyrone Hinderson
 */
public class IStreamDelegatingDecompressorStreamTest {
    private IStreamDelegatingDecompressorStream decompressorStream;
    private InputStream mockIStream = Mockito.mock(InputStream.class);

    @Before
    public void setUp() throws Exception {
        decompressorStream = new IStreamDelegatingDecompressorStream(mockIStream);
    }

    @Test
    public void testRead() throws Exception {
        decompressorStream.read();
        Mockito.verify(mockIStream, Mockito.times(1)).read();
    }

    @Test
    public void testRead1() throws Exception {
        final byte[] bytes = { 1, 2, 3 };
        decompressorStream.read(bytes, 0, 0);
        Mockito.verify(mockIStream, Mockito.times(1)).read(bytes, 0, 0);
    }
}
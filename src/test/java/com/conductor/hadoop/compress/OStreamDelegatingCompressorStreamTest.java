package com.conductor.hadoop.compress;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.OutputStream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for the {@link OStreamDelegatingCompressorStream}
 * <p/>
 * 2014
 *
 * @author Tyrone Hinderson
 */
public class OStreamDelegatingCompressorStreamTest {
    private OStreamDelegatingCompressorStream compressorStream;
    private OutputStream mockOStream = Mockito.mock(OutputStream.class);

    @Before
    public void setUp() throws Exception {
        compressorStream = new OStreamDelegatingCompressorStream(mockOStream);
    }

    @Test
    public void testWrite() throws Exception {
        compressorStream.write(0);
        verify(mockOStream, Mockito.times(1)).write(0);
    }

    @Test
    public void testWrite1() throws Exception {
        final byte[] bytes = { 1, 2 };
        compressorStream.write(bytes, 0, 0);
        verify(mockOStream, times(1)).write(bytes, 0, 0);
    }

    @Test
    public void testClose() throws Exception {
        compressorStream.close();
        verify(mockOStream, times(1)).flush();
        verify(mockOStream, times(1)).close();
    }

    @Test
    public void testFlush() throws Exception {
        compressorStream.flush();
        verify(mockOStream, times(1)).flush();
    }
}
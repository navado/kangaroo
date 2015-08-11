package com.conductor.hadoop.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link SnappyFramedCodec}.
 */
public class SnappyFramedCodecTest {
    final Configuration testConf = new Configuration();
    final SnappyFramedCodec subject = new SnappyFramedCodec(testConf);

    @Test
    public void testCompressAndDecompressConsistent() throws Exception {
        final String testString = "Test String";
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final OutputStreamWriter writer = new OutputStreamWriter(subject.createOutputStream(baos));
        writer.write(testString);
        writer.flush();
        writer.close();

        final CompressionInputStream inputStream = subject.createInputStream(new ByteArrayInputStream(baos
                .toByteArray()));
        final StringWriter contentsTester = new StringWriter();
        IOUtils.copy(inputStream, contentsTester);
        inputStream.close();
        contentsTester.flush();
        contentsTester.close();

        Assert.assertEquals(testString, contentsTester.toString());
    }

    @Test
    public void testGetCompressorType() throws Exception {
        Assert.assertNull(subject.getCompressorType());
    }

    @Test
    public void testCreateCompressor() throws Exception {
        Assert.assertNull(subject.createCompressor());
    }

    @Test
    public void testGetDecompressorType() throws Exception {
        Assert.assertNull(subject.getDecompressorType());
    }

    @Test
    public void testCreateDecompressor() throws Exception {
        Assert.assertNull(subject.createDecompressor());
    }
}
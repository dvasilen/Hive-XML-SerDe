/**
 * IBM Confidential
 * 
 * OCO Source Materials
 * 
 * IBM SPSS Products: Analytic Engine
 * (c) Copyright IBM Corp. 2013
 * 
 * The source code for this program is not published or otherwise divested of
 * its trade secrets, irrespective of what has been deposited with the U.S.
 * Copyright Office.
 */

package com.ibm.spss.hive.serde2.xml;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

public class HiveXmlRecordReaderTest extends TestCase {

    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public HiveXmlRecordReaderTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(HiveXmlRecordReaderTest.class);
    }

    private static final String XML = "...<record>record1</record>...<record>record2</record>...";

    public void test() throws Exception {
        JobConf jobConf = new JobConf();
        jobConf.set(HiveXmlRecordReader.START_TAG_KEY, "<record>");
        jobConf.set(HiveXmlRecordReader.END_TAG_KEY, "</record>");
        InputStream inputstream = new ByteArrayInputStream(XML.getBytes());
        long start = 0;
        long end = 1024;
        HiveXmlRecordReader hxrr = new HiveXmlRecordReader(jobConf, inputstream, start, end);
        LongWritable key = hxrr.createKey();
        Text value = hxrr.createValue();
        hxrr.next(key, value);
        assertEquals("<record>record1</record>", value.toString());
        key = hxrr.createKey();
        value = hxrr.createValue();
        hxrr.next(key, value);
        assertEquals("<record>record2</record>", value.toString());
    }

}
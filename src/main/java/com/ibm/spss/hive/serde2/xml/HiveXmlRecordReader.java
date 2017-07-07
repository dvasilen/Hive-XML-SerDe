/**
 * (c) Copyright IBM Corp. 2013. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.ibm.spss.hive.serde2.xml;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Given the stream reads records that are delimited by a specific begin/end tag.
 */
public class HiveXmlRecordReader implements RecordReader<LongWritable, Text> {

    public static final String START_TAG_KEY = "xmlinput.start";
    public static final String END_TAG_KEY = "xmlinput.end";

    private byte[] startTag;
    private byte[] endTag;
    private final long start;
    private final long end;
    private long pos;
    private InputStream inputstream;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private long recordStartPos;

    /**
     * Creates Hive XML record reader
     * 
     * @param jobConf
     *            the job configuration
     * @param inputstream
     *            the input stream (decompressed)
     * @param start
     *            the start of the split
     * @param end
     *            the end of the split
     * @throws IOException
     *             as appropriate
     */
    public HiveXmlRecordReader(JobConf jobConf, InputStream inputstream, long start, long end) throws IOException {
        this.inputstream = inputstream;
        this.startTag = jobConf.get(START_TAG_KEY).getBytes("utf-8");
        this.endTag = jobConf.get(END_TAG_KEY).getBytes("utf-8");
        this.start = start;
        this.end = end;
        this.recordStartPos = this.start;
        this.pos = this.start;
    }

    @Override
    public boolean next(LongWritable key, Text value) throws IOException {

        if (readUntilMatch(this.startTag, false)) {
            this.recordStartPos = this.pos - this.startTag.length;
            try {
                this.buffer.write(this.startTag);
                if (readUntilMatch(this.endTag, true)) {
                    key.set(this.recordStartPos);
                    value.set(this.buffer.getData(), 0, this.buffer.getLength());
                    return true;
                }
            } finally {
                this.buffer.reset();
            }
        }

        return false;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public void close() throws IOException {
        this.inputstream.close();
    }

    @Override
    public float getProgress() throws IOException {
        return ((float) (this.pos - this.start)) / ((float) (this.end - this.start));
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock) {
        int i = 0;
        try {
            while (true) {
                int b = this.inputstream.read();
                ++this.pos;

                if (b == -1) {
                    return false;
                }
                if (withinBlock) {
                    this.buffer.write(b);
                }
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) {
                        return true;
                    }
                } else {
                    i = 0;
                }
            }
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public long getPos() throws IOException {
        return this.pos;
    }
}

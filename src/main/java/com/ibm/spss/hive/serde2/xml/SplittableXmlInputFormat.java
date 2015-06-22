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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class SplittableXmlInputFormat extends TextInputFormat {

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf job, Reporter reporter) throws IOException {

        InputStream inputStream = null;
        try {
            inputStream = getInputStream(job, (FileSplit) inputSplit);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        long start = ((FileSplit) inputSplit).getStart();
        long end = start + inputSplit.getLength();

        return new HiveXmlRecordReader(job, inputStream, start, end);
    }

    private InputStream getInputStream(JobConf jobConf, FileSplit split) throws IOException, ClassNotFoundException {
        FSDataInputStream fsin = null;

        // open the file and seek to the start of the split
        long splitStart = split.getStart();
        long splitEnd = splitStart + split.getLength();
        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(jobConf);
        fsin = fs.open(split.getPath());
        fsin.seek(splitStart);

        Configuration conf = new Configuration();
        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(conf);
        CompressionCodec codec = compressionCodecFactory.getCodec(split.getPath());
        Decompressor decompressor = CodecPool.getDecompressor(codec);
        if (codec instanceof SplittableCompressionCodec) {
            return ((SplittableCompressionCodec) codec).createInputStream(fsin,
                decompressor,
                splitStart,
                splitEnd,
                SplittableCompressionCodec.READ_MODE.BYBLOCK);
        } else {
            return codec.createInputStream(fsin, decompressor);
        }
    }
}
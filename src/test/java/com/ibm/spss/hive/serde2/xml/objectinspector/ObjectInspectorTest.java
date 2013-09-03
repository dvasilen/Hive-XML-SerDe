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

package com.ibm.spss.hive.serde2.xml.objectinspector;

import java.util.Map;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;

import com.ibm.spss.hive.serde2.xml.XmlSerDe;
import com.ibm.spss.hive.serde2.xml.processor.java.NodeArray;

/**
 * 
 */
public class ObjectInspectorTest extends TestCase {

    private static final String LIST_COLUMNS = "columns";
    private static final String LIST_COLUMN_TYPES = "columns.types";
    
    
    public ObjectInspectorTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(ObjectInspectorTest.class);
    }

    public void testSimpleXmlBoolean() throws SerDeException {
        XmlSerDe xmlSerDe = new XmlSerDe();
        Configuration configuration = new Configuration();
        Properties properties = new Properties();
        properties.put(LIST_COLUMNS, "test");
        properties.put(LIST_COLUMN_TYPES, "boolean");
        properties.setProperty("column.xpath.test", "/test/text()");
        xmlSerDe.initialize(configuration, properties);
        Text text = new Text();
        text.set("<test>true</test>");
        Object o = xmlSerDe.deserialize(text);
        XmlStructObjectInspector structInspector = ((XmlStructObjectInspector) xmlSerDe.getObjectInspector());
        StructField structField = structInspector.getStructFieldRef("test");
        Object data = (Boolean) structInspector.getStructFieldData(o, structField);
        assertEquals(true, data);
    }

    public void testSimpleXmlByte() throws SerDeException {
        XmlSerDe xmlSerDe = new XmlSerDe();
        Configuration configuration = new Configuration();
        Properties properties = new Properties();
        properties.put(LIST_COLUMNS, "test");
        properties.put(LIST_COLUMN_TYPES, "tinyint");
        properties.setProperty("column.xpath.test", "/test/text()");
        xmlSerDe.initialize(configuration, properties);
        Text text = new Text();
        text.set("<test>14</test>");
        Object o = xmlSerDe.deserialize(text);
        XmlStructObjectInspector structInspector = ((XmlStructObjectInspector) xmlSerDe.getObjectInspector());
        StructField structField = structInspector.getStructFieldRef("test");
        Object data = structInspector.getStructFieldData(o, structField);
        assertEquals((byte) 14, ((Byte) data).byteValue());
    }

    public void testSimpleXmlDouble() throws SerDeException {
        XmlSerDe xmlSerDe = new XmlSerDe();
        Configuration configuration = new Configuration();
        Properties properties = new Properties();
        properties.put(LIST_COLUMNS, "test");
        properties.put(LIST_COLUMN_TYPES, "double");
        properties.setProperty("column.xpath.test", "/test/text()");
        xmlSerDe.initialize(configuration, properties);
        Text text = new Text();
        text.set("<test>123.456</test>");
        Object o = xmlSerDe.deserialize(text);
        XmlStructObjectInspector structInspector = ((XmlStructObjectInspector) xmlSerDe.getObjectInspector());
        StructField structField = structInspector.getStructFieldRef("test");
        Object data = structInspector.getStructFieldData(o, structField);
        assertEquals(123.456, data);
    }

    public void testSimpleXmlFloat() throws SerDeException {
        XmlSerDe xmlSerDe = new XmlSerDe();
        Configuration configuration = new Configuration();
        Properties properties = new Properties();
        properties.put(LIST_COLUMNS, "test");
        properties.put(LIST_COLUMN_TYPES, "float");
        properties.setProperty("column.xpath.test", "/test/text()");
        xmlSerDe.initialize(configuration, properties);
        Text text = new Text();
        text.set("<test>123.456</test>");
        Object o = xmlSerDe.deserialize(text);
        XmlStructObjectInspector structInspector = ((XmlStructObjectInspector) xmlSerDe.getObjectInspector());
        StructField structField = structInspector.getStructFieldRef("test");
        Object data = structInspector.getStructFieldData(o, structField);
        assertEquals(123.456f, data);
    }

    public void testSimpleXmlInt() throws SerDeException {
        XmlSerDe xmlSerDe = new XmlSerDe();
        Configuration configuration = new Configuration();
        Properties properties = new Properties();
        properties.put(LIST_COLUMNS, "test");
        properties.put(LIST_COLUMN_TYPES, "int");
        properties.setProperty("column.xpath.test", "/test/text()");
        xmlSerDe.initialize(configuration, properties);
        Text text = new Text();
        text.set("<test>23</test>");
        Object o = xmlSerDe.deserialize(text);
        XmlStructObjectInspector structInspector = ((XmlStructObjectInspector) xmlSerDe.getObjectInspector());
        StructField structField = structInspector.getStructFieldRef("test");
        Object data = structInspector.getStructFieldData(o, structField);
        assertEquals(23, data);
    }

    public void testSimpleXmlLong() throws SerDeException {
        XmlSerDe xmlSerDe = new XmlSerDe();
        Configuration configuration = new Configuration();
        Properties properties = new Properties();
        properties.put(LIST_COLUMNS, "test");
        properties.put(LIST_COLUMN_TYPES, "bigint");
        properties.setProperty("column.xpath.test", "/test/text()");
        xmlSerDe.initialize(configuration, properties);
        Text text = new Text();
        text.set("<test>123456</test>");
        Object o = xmlSerDe.deserialize(text);
        XmlStructObjectInspector structInspector = ((XmlStructObjectInspector) xmlSerDe.getObjectInspector());
        StructField structField = structInspector.getStructFieldRef("test");
        Object data = structInspector.getStructFieldData(o, structField);
        assertEquals(123456l, data);
    }

    public void testSimpleXmlShort() throws SerDeException {
        XmlSerDe xmlSerDe = new XmlSerDe();
        Configuration configuration = new Configuration();
        Properties properties = new Properties();
        properties.put(LIST_COLUMNS, "test");
        properties.put(LIST_COLUMN_TYPES, "smallint");
        properties.setProperty("column.xpath.test", "/test/text()");
        xmlSerDe.initialize(configuration, properties);
        Text text = new Text();
        text.set("<test>12</test>");
        Object o = xmlSerDe.deserialize(text);
        XmlStructObjectInspector structInspector = ((XmlStructObjectInspector) xmlSerDe.getObjectInspector());
        StructField structField = structInspector.getStructFieldRef("test");
        Object data = structInspector.getStructFieldData(o, structField);
        assertEquals((short) 12, ((Short) data).shortValue());
    }

    public void testSimpleXmlString() throws SerDeException {
        XmlSerDe xmlSerDe = new XmlSerDe();
        Configuration configuration = new Configuration();
        Properties properties = new Properties();
        properties.put(LIST_COLUMNS, "test");
        properties.put(LIST_COLUMN_TYPES, "string");
        properties.setProperty("column.xpath.test", "/test/text()");
        xmlSerDe.initialize(configuration, properties);
        Text text = new Text();
        text.set("<test>string</test>");
        Object o = xmlSerDe.deserialize(text);
        XmlStructObjectInspector structInspector = ((XmlStructObjectInspector) xmlSerDe.getObjectInspector());
        StructField structField = structInspector.getStructFieldRef("test");
        Object data = structInspector.getStructFieldData(o, structField);
        assertEquals("string", data.toString());
    }

    @SuppressWarnings("rawtypes")
    public void testSimpleXmlList() throws SerDeException {
         XmlSerDe xmlSerDe = new XmlSerDe();
         Configuration configuration = new Configuration();
         Properties properties = new Properties();
         properties.put(LIST_COLUMNS, "test");
         properties.put(LIST_COLUMN_TYPES, "array<string>");
         properties.setProperty("column.xpath.test", "//test/text()");
         xmlSerDe.initialize(configuration, properties);
         Text text = new Text();
         text.set("<root><test>string1</test><test>string2</test></root>");
         Object o = xmlSerDe.deserialize(text);
         XmlStructObjectInspector structInspector = ((XmlStructObjectInspector) xmlSerDe.getObjectInspector());
         StructField structField = structInspector.getStructFieldRef("test");
         Object data = structInspector.getStructFieldData(o, structField);
         assertEquals("string1", ((NodeArray)data).get(0).getNodeValue());
    }

    @SuppressWarnings("rawtypes")
    public void testSimpleXmlMap() throws SerDeException {
         XmlSerDe xmlSerDe = new XmlSerDe();
         Configuration configuration = new Configuration();
         Properties properties = new Properties();
         properties.put(LIST_COLUMNS, "test");
         properties.put(LIST_COLUMN_TYPES, "map<string,string>");
         properties.setProperty("column.xpath.test", "//*[contains(name(),'test')]");
         xmlSerDe.initialize(configuration, properties);
         Text text = new Text();
         text.set("<root><test1>string1</test1><test2>string2</test2></root>");
         Object o = xmlSerDe.deserialize(text);
         XmlStructObjectInspector structInspector = ((XmlStructObjectInspector) xmlSerDe.getObjectInspector());
         StructField structField = structInspector.getStructFieldRef("test");
         Object data = structInspector.getStructFieldData(o, structField);
         XmlMapObjectInspector fieldInspector = (XmlMapObjectInspector) structField.getFieldObjectInspector();
         Map map = fieldInspector.getMap(data);
         PrimitiveObjectInspector valueObjectInspector = (PrimitiveObjectInspector) fieldInspector.getMapValueObjectInspector();
         String test = (String) valueObjectInspector.getPrimitiveJavaObject(map.get("test1"));
         assertEquals("string1", test);
    }

    @SuppressWarnings("rawtypes")
    public void testSimpleXmlNotMap() throws SerDeException {
        XmlSerDe xmlSerDe = new XmlSerDe();
        Configuration configuration = new Configuration();
        Properties properties = new Properties();
        properties.put(LIST_COLUMNS, "test");
        properties.put(LIST_COLUMN_TYPES, "map<string,string>");
        properties.setProperty("column.xpath.test", "//*[contains(name(),'test')]/text()");
        xmlSerDe.initialize(configuration, properties);
        Text text = new Text();
        text.set("<root><test1>string1</test1><test2>string2</test2></root>");
        Object o = xmlSerDe.deserialize(text);
        XmlStructObjectInspector structInspector = ((XmlStructObjectInspector) xmlSerDe.getObjectInspector());
        StructField structField = structInspector.getStructFieldRef("test");
        Object data = structInspector.getStructFieldData(o, structField);
        XmlMapObjectInspector fieldInspector = (XmlMapObjectInspector) structField.getFieldObjectInspector();
        Map map = fieldInspector.getMap(data);
        assertEquals(0, map.size());
    }

}
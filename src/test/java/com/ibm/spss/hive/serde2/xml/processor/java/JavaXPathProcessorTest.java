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

package com.ibm.spss.hive.serde2.xml.processor.java;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.w3c.dom.Node;

import com.ibm.spss.hive.serde2.xml.processor.XmlMapEntry;
import com.ibm.spss.hive.serde2.xml.processor.XmlMapFacet;
import com.ibm.spss.hive.serde2.xml.processor.XmlProcessorContext;
import com.ibm.spss.hive.serde2.xml.processor.XmlQuery;

/**
 * 
 */
public class JavaXPathProcessorTest extends TestCase {

    public JavaXPathProcessorTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(JavaXPathProcessorTest.class);
    }

    public void testSimpleAttribute() {
        final List<XmlQuery> queries = Arrays.asList(new XmlQuery[] {new XmlQuery("/root/@a", "field1")});
        String data = "<root a=\"value\"/>";
        JavaXmlProcessor xPathProcessor = new JavaXmlProcessor();
        xPathProcessor.initialize(new XmlProcessorContext() {

            @Override
            public List<XmlQuery> getXmlQueries() {
                return queries;
            }

            @Override
            public Map<String, XmlMapEntry> getXmlMapSpecification() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Properties getProperties() {
                // TODO Auto-generated method stub
                return null;
            }

        });
        Map<String, NodeArray> out = xPathProcessor.parse(data);
        Node node = out.get("field1").get(0);
        assertTrue(node.getNodeType() == Node.ATTRIBUTE_NODE);
        assertEquals(xPathProcessor.getPrimitiveObjectValue(node, PrimitiveCategory.STRING), "value");
    }

    public void testSimpleElement() {
        final List<XmlQuery> queries = Arrays.asList(new XmlQuery[] {new XmlQuery("/root[@a='value']", "field1")});
        String data = "<root a=\"value\"/>";
        JavaXmlProcessor xPathProcessor = new JavaXmlProcessor();
        xPathProcessor.initialize(new XmlProcessorContext() {

            @Override
            public List<XmlQuery> getXmlQueries() {
                return queries;
            }

            @Override
            public Map<String, XmlMapEntry> getXmlMapSpecification() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Properties getProperties() {
                // TODO Auto-generated method stub
                return null;
            }

        });
        Map<String, NodeArray> out = xPathProcessor.parse(data);
        Node node = out.get("field1").get(0);
        assertTrue(node.getNodeType() == Node.ELEMENT_NODE);
        assertEquals("<root a=\"value\"/>", xPathProcessor.getPrimitiveObjectValue(node, PrimitiveCategory.STRING));
    }

    public void testSimpleArrayElement() {
        final List<XmlQuery> queries = Arrays.asList(new XmlQuery[] {new XmlQuery("/root/item", "field1")});
        String data = "<root><item a=\"value\"/><item a=\"value\"/></root>";
        JavaXmlProcessor xPathProcessor = new JavaXmlProcessor();
        xPathProcessor.initialize(new XmlProcessorContext() {

            @Override
            public List<XmlQuery> getXmlQueries() {
                return queries;
            }

            @Override
            public Map<String, XmlMapEntry> getXmlMapSpecification() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Properties getProperties() {
                // TODO Auto-generated method stub
                return null;
            }

        });

        Map<String, NodeArray> out = xPathProcessor.parse(data);
        Node node = out.get("field1").get(0);
        assertTrue(node.getNodeType() == Node.ELEMENT_NODE);
    }

    public void testSimpleArrayElementWithText() {
        final List<XmlQuery> queries = Arrays.asList(new XmlQuery[] {new XmlQuery("/root/item", "field1")});
        String data = "<root><item a=\"value\">TEXT1</item><item a=\"value\">TEXT2</item></root>";
        JavaXmlProcessor xPathProcessor = new JavaXmlProcessor();
        xPathProcessor.initialize(new XmlProcessorContext() {

            @Override
            public List<XmlQuery> getXmlQueries() {
                return queries;
            }

            @Override
            public Map<String, XmlMapEntry> getXmlMapSpecification() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Properties getProperties() {
                // TODO Auto-generated method stub
                return null;
            }

        });
        Map<String, NodeArray> out = xPathProcessor.parse(data);
        Node node = out.get("field1").get(0);
        assertTrue(node.getNodeType() == Node.ELEMENT_NODE);
    }

    public void testSimpleMapElementAsDefaultElementToContent() {
        final List<XmlQuery> queries = Arrays.asList(new XmlQuery[] {new XmlQuery("/root/node()", "field1")});
        String data = "<root><key1>value1</key1><key2>value2</key2></root>";
        JavaXmlProcessor xPathProcessor = new JavaXmlProcessor();
        xPathProcessor.initialize(new XmlProcessorContext() {

            @Override
            public List<XmlQuery> getXmlQueries() {
                return queries;
            }

            @Override
            public Map<String, XmlMapEntry> getXmlMapSpecification() {
                Map<String, XmlMapEntry> spec = new HashMap<String, XmlMapEntry>();
                return spec;
            }

            @Override
            public Properties getProperties() {
                // TODO Auto-generated method stub
                return null;
            }

        });

        Map<String, NodeArray> out = xPathProcessor.parse(data);
        NodeArray nodeArray = out.get("field1");
        Map map = xPathProcessor.getMap(nodeArray);
        assertEquals("value1", ((NodeArray) map.get("key1")).get(0).getNodeValue());
        assertEquals("value2", ((NodeArray) map.get("key2")).get(0).getNodeValue());
    }

    public void testSimpleMapElementAsContentToElement() {
        final List<XmlQuery> queries = Arrays.asList(new XmlQuery[] {new XmlQuery("/root/node()", "field1")});
        String data = "<root><value1>key1</value1><value2>key2</value2></root>";
        JavaXmlProcessor xPathProcessor = new JavaXmlProcessor();
        xPathProcessor.initialize(new XmlProcessorContext() {

            @Override
            public List<XmlQuery> getXmlQueries() {
                return queries;
            }

            @Override
            public Map<String, XmlMapEntry> getXmlMapSpecification() {
                Map<String, XmlMapEntry> spec = new HashMap<String, XmlMapEntry>();
                // value1=#content->value1
                spec.put("value1", new XmlMapEntry(new XmlMapFacet("content", XmlMapFacet.Type.CONTENT), new XmlMapFacet("value1",
                    XmlMapFacet.Type.ELEMENT)));
                spec.put("value2", new XmlMapEntry(new XmlMapFacet("content", XmlMapFacet.Type.CONTENT), new XmlMapFacet("value2",
                    XmlMapFacet.Type.ELEMENT)));
                return spec;
            }

            @Override
            public Properties getProperties() {
                // TODO Auto-generated method stub
                return null;
            }

        });

        Map<String, NodeArray> out = xPathProcessor.parse(data);
        NodeArray nodeArray = out.get("field1");
        Map map = xPathProcessor.getMap(nodeArray);
        assertEquals("value1", map.get("key1"));
        assertEquals("value2", map.get("key2"));
    }

    public void testSimpleMapElementAsAttributeToAttribute() {
        final List<XmlQuery> queries = Arrays.asList(new XmlQuery[] {new XmlQuery("/root/node()", "field1")});
        String data = "<root><element key=\"key1\" value=\"value1\"/></root>";
        JavaXmlProcessor xPathProcessor = new JavaXmlProcessor();
        xPathProcessor.initialize(new XmlProcessorContext() {

            @Override
            public List<XmlQuery> getXmlQueries() {
                return queries;
            }

            @Override
            public Map<String, XmlMapEntry> getXmlMapSpecification() {
                Map<String, XmlMapEntry> spec = new HashMap<String, XmlMapEntry>();
                // element=@key->@value
                spec.put("element", new XmlMapEntry(new XmlMapFacet("key", XmlMapFacet.Type.ATTRIBUTE), new XmlMapFacet("value",
                    XmlMapFacet.Type.ATTRIBUTE)));
                return spec;
            }

            @Override
            public Properties getProperties() {
                // TODO Auto-generated method stub
                return null;
            }

        });

        Map<String, NodeArray> out = xPathProcessor.parse(data);
        NodeArray nodeArray = out.get("field1");
        Map map = xPathProcessor.getMap(nodeArray);
        assertEquals("value1", map.get("key1"));
    }

    public void testSimpleMapElementAsAttributeToContent() {
        final List<XmlQuery> queries = Arrays.asList(new XmlQuery[] {new XmlQuery("/root/node()", "field1")});
        String data = "<root><element key=\"key1\">value1</element></root>";
        JavaXmlProcessor xPathProcessor = new JavaXmlProcessor();
        xPathProcessor.initialize(new XmlProcessorContext() {

            @Override
            public List<XmlQuery> getXmlQueries() {
                return queries;
            }

            @Override
            public Map<String, XmlMapEntry> getXmlMapSpecification() {
                Map<String, XmlMapEntry> spec = new HashMap<String, XmlMapEntry>();
                // element=@key->#content
                spec.put("element", new XmlMapEntry(new XmlMapFacet("key", XmlMapFacet.Type.ATTRIBUTE), new XmlMapFacet("value",
                    XmlMapFacet.Type.CONTENT)));
                return spec;
            }

            @Override
            public Properties getProperties() {
                // TODO Auto-generated method stub
                return null;
            }

        });

        Map<String, NodeArray> out = xPathProcessor.parse(data);
        NodeArray nodeArray = out.get("field1");
        Map map = xPathProcessor.getMap(nodeArray);
        assertEquals("value1", (((NodeArray) map.get("key1"))).get(0).getNodeValue());
    }

    public void testSimpleMapElementAsContentToAttribute() {
        final List<XmlQuery> queries = Arrays.asList(new XmlQuery[] {new XmlQuery("/root/node()", "field1")});
        String data = "<root><element value=\"value1\">key1</element></root>";
        JavaXmlProcessor xPathProcessor = new JavaXmlProcessor();
        xPathProcessor.initialize(new XmlProcessorContext() {

            @Override
            public List<XmlQuery> getXmlQueries() {
                return queries;
            }

            @Override
            public Map<String, XmlMapEntry> getXmlMapSpecification() {
                Map<String, XmlMapEntry> spec = new HashMap<String, XmlMapEntry>();
                // element=#content->@value
                spec.put("element", new XmlMapEntry(new XmlMapFacet(null, XmlMapFacet.Type.CONTENT), new XmlMapFacet("value",
                    XmlMapFacet.Type.ATTRIBUTE)));
                return spec;
            }

            @Override
            public Properties getProperties() {
                // TODO Auto-generated method stub
                return null;
            }

        });

        Map<String, NodeArray> out = xPathProcessor.parse(data);
        NodeArray nodeArray = out.get("field1");
        Map map = xPathProcessor.getMap(nodeArray);
        assertEquals("value1", map.get("key1"));
    }

    public void testSimpleMapElementAsElementToAttribute() {
        final List<XmlQuery> queries = Arrays.asList(new XmlQuery[] {new XmlQuery("/root/node()", "field1")});
        String data = "<root><key1 value=\"value1\"/></root>";
        JavaXmlProcessor xPathProcessor = new JavaXmlProcessor();
        xPathProcessor.initialize(new XmlProcessorContext() {

            @Override
            public List<XmlQuery> getXmlQueries() {
                return queries;
            }

            @Override
            public Map<String, XmlMapEntry> getXmlMapSpecification() {
                Map<String, XmlMapEntry> spec = new HashMap<String, XmlMapEntry>();
                // key1=key1->@value
                spec.put("key1", new XmlMapEntry(new XmlMapFacet("key1", XmlMapFacet.Type.ELEMENT), new XmlMapFacet("value",
                    XmlMapFacet.Type.ATTRIBUTE)));
                return spec;
            }

            @Override
            public Properties getProperties() {
                // TODO Auto-generated method stub
                return null;
            }

        });

        Map<String, NodeArray> out = xPathProcessor.parse(data);
        NodeArray nodeArray = out.get("field1");
        Map map = xPathProcessor.getMap(nodeArray);
        assertEquals("value1", map.get("key1"));
    }

    public void testSimpleMapElementAsAttributeToElement() {
        final List<XmlQuery> queries = Arrays.asList(new XmlQuery[] {new XmlQuery("/root/node()", "field1")});
        String data = "<root><value1 key=\"key1\"/></root>";
        JavaXmlProcessor xPathProcessor = new JavaXmlProcessor();
        xPathProcessor.initialize(new XmlProcessorContext() {

            @Override
            public List<XmlQuery> getXmlQueries() {
                return queries;
            }

            @Override
            public Map<String, XmlMapEntry> getXmlMapSpecification() {
                Map<String, XmlMapEntry> spec = new HashMap<String, XmlMapEntry>();
                // value1=@key->value1
                spec.put("value1", new XmlMapEntry(new XmlMapFacet("key", XmlMapFacet.Type.ATTRIBUTE), new XmlMapFacet("value1",
                    XmlMapFacet.Type.ELEMENT)));
                return spec;
            }

            @Override
            public Properties getProperties() {
                // TODO Auto-generated method stub
                return null;
            }

        });

        Map<String, NodeArray> out = xPathProcessor.parse(data);
        NodeArray nodeArray = out.get("field1");
        Map map = xPathProcessor.getMap(nodeArray);
        assertEquals("value1", map.get("key1"));
    }

}
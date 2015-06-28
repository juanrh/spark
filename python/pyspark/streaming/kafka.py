#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from py4j.java_gateway import Py4JJavaError
from collections import namedtuple

from pyspark.rdd import RDD
from pyspark.storagelevel import StorageLevel
from pyspark.serializers import PairDeserializer, NoOpSerializer
from pyspark.streaming import DStream


__all__ = ['Broker', 'KafkaUtils', 'OffsetRange', 'TopicAndPartition', 'utf8_decoder']


def utf8_decoder(s):
    """ Decode the unicode as UTF-8 """
    return s and s.decode('utf-8')

_MessageAndMetadata = namedtuple("MessageAndMetadata", ["key", "value", "topic", "partition", "offset"])

class JFunction(object):
    def __init__(self, f):
        self._f = f

    def call(self, v):
        return self._f(v)

    class Java:
        implements = ['org.apache.spark.api.java.function.Function']

class KafkaUtils(object):

    @staticmethod
    def createStream(ssc, zkQuorum, groupId, topics, kafkaParams={},
                     storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2,
                     keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):
        """
        Create an input stream that pulls messages from a Kafka Broker.

        :param ssc:  StreamingContext object
        :param zkQuorum:  Zookeeper quorum (hostname:port,hostname:port,..).
        :param groupId:  The group id for this consumer.
        :param topics:  Dict of (topic_name -> numPartitions) to consume.
                        Each partition is consumed in its own thread.
        :param kafkaParams: Additional params for Kafka
        :param storageLevel:  RDD storage level.
        :param keyDecoder:  A function used to decode key (default is utf8_decoder)
        :param valueDecoder:  A function used to decode value (default is utf8_decoder)
        :return: A DStream object
        """
        kafkaParams.update({
            "zookeeper.connect": zkQuorum,
            "group.id": groupId,
            "zookeeper.connection.timeout.ms": "10000",
        })
        if not isinstance(topics, dict):
            raise TypeError("topics should be dict")
        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)

        try:
            # Use KafkaUtilsPythonHelper to access Scala's KafkaUtils (see SPARK-6027)
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader()\
                .loadClass("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper")
            helper = helperClass.newInstance()
            jstream = helper.createStream(ssc._jssc, kafkaParams, topics, jlevel)
        except Py4JJavaError as e:
            # TODO: use --jar once it also work on driver
            if 'ClassNotFoundException' in str(e.java_exception):
                KafkaUtils._printErrorMsg(ssc.sparkContext)
            raise e
        ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())
        stream = DStream(jstream, ssc, ser)
        return stream.map(lambda k_v: (keyDecoder(k_v[0]), valueDecoder(k_v[1])))

    @staticmethod
    def createDirectStream(ssc, topics, kafkaParams, fromOffsets={},
                           keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):
        """
        .. note:: Experimental

        Create an input stream that directly pulls messages from a Kafka Broker and specific offset.

        This is not a receiver based Kafka input stream, it directly pulls the message from Kafka
        in each batch duration and processed without storing.

        This does not use Zookeeper to store offsets. The consumed offsets are tracked
        by the stream itself. For interoperability with Kafka monitoring tools that depend on
        Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
        You can access the offsets used in each batch from the generated RDDs (see

        To recover from driver failures, you have to enable checkpointing in the StreamingContext.
        The information on consumed offset can be recovered from the checkpoint.
        See the programming guide for details (constraints, etc.).

        :param ssc:  StreamingContext object.
        :param topics:  list of topic_name to consume.
        :param kafkaParams: Additional params for Kafka.
        :param fromOffsets: Per-topic/partition Kafka offsets defining the (inclusive) starting
                            point of the stream.
        :param keyDecoder:  A function used to decode key (default is utf8_decoder).
        :param valueDecoder:  A function used to decode value (default is utf8_decoder).
        :return: A DStream object
        """
        if not isinstance(topics, list):
            raise TypeError("topics should be list")
        if not isinstance(kafkaParams, dict):
            raise TypeError("kafkaParams should be dict")

        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper")
            helper = helperClass.newInstance()

            jfromOffsets = dict([(k._jTopicAndPartition(helper),
                                  v) for (k, v) in fromOffsets.items()])
            jstream = helper.createDirectStream(ssc._jssc, kafkaParams, set(topics), jfromOffsets)
        except Py4JJavaError as e:
            if 'ClassNotFoundException' in str(e.java_exception):
                KafkaUtils._printErrorMsg(ssc.sparkContext)
            raise e

        ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())
        stream = DStream(jstream, ssc, ser)
        return stream.map(lambda k_v: (keyDecoder(k_v[0]), valueDecoder(k_v[1])))

    @staticmethod
    def createDirectStreamMM(ssc, topics, kafkaParams, fromOffsets={},
                           keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):
        """
        FIXME: temporary working placeholder
        """
        if not isinstance(topics, list):
            raise TypeError("topics should be list")
        if not isinstance(kafkaParams, dict):
            raise TypeError("kafkaParams should be dict")

        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper")
            helper = helperClass.newInstance()

            jfromOffsets = dict([(k._jTopicAndPartition(helper),
                                  v) for (k, v) in fromOffsets.items()])
            jstream = helper.createDirectStreamMM(ssc._jssc, kafkaParams, set(topics), jfromOffsets)
        except Py4JJavaError as e:
            if 'ClassNotFoundException' in str(e.java_exception):
                KafkaUtils._printErrorMsg(ssc.sparkContext)
            raise e

        ser = NoOpSerializer()
        stream = DStream(jstream, ssc, ser)
        return stream
        # return stream.map(lambda k_v: (keyDecoder(k_v[0]), valueDecoder(k_v[1])))

    @staticmethod
    def createDirectStreamF(ssc, topics, kafkaParams, fromOffsets={},
                            messageHandler=None, 
                           keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):
        """
        FIXME: temporary working placeholder

         def createDirectStreamF[R](
            jssc: JavaStreamingContext,
            resultClass : Class[R], 
            kafkaParams: JMap[String, String],
            topics: JSet[String],
            fromOffsets: JMap[TopicAndPartition, JLong], 
            messageHandler :  JFunction[MessageAndMetadata[Array[Byte], Array[Byte]],R] 
            ): JavaInputDStream[R] = {
        """
        if not isinstance(topics, list):
            raise TypeError("topics should be list")
        if not isinstance(kafkaParams, dict):
            raise TypeError("kafkaParams should be dict")

        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper")
            helper = helperClass.newInstance()

            jfromOffsets = dict([(k._jTopicAndPartition(helper),
                                  v) for (k, v) in fromOffsets.items()])
            messageAndMetadataClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("kafka.message.MessageAndMetadata")
            jfunctionClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.api.java.function.Function")
            pyJFun = JFunction(lambda x : x)
            # Returning always MessageAndMetadata in this example
            jstream = helper.createDirectStreamF(ssc._jssc, messageAndMetadataClass, kafkaParams, set(topics), jfromOffsets, pyJFun)
        except Py4JJavaError as e:
            if 'ClassNotFoundException' in str(e.java_exception):
                KafkaUtils._printErrorMsg(ssc.sparkContext)
            raise e

        # we need to use a Deserializer for entries ((key, message), (topic, (partition, offset)))
        ser = PairDeserializer(PairDeserializer(NoOpSerializer(), NoOpSerializer()),\
                               PairDeserializer(NoOpSerializer(), \
                                                PairDeserializer(NoOpSerializer(), NoOpSerializer()) 
                                                ) 
                               )
        stream = DStream(jstream, ssc, ser)
        def getMetadataAndDecode(jmsgAndMetadata):
            # Note hardcoding class name as this is @staticmethod
            # and not @classmethod
            return _MessageAndMetadata(
                    key = keyDecoder(jmsgAndMetadata[0][0]),
                    value = valueDecoder(jmsgAndMetadata[0][1]),
                    topic = utf8_decoder(jmsgAndMetadata[1][0]),
                    partition = int(utf8_decoder(jmsgAndMetadata[1][1][0])), 
                    offset = long(utf8_decoder(jmsgAndMetadata[1][1][1]))
                   )
            '''
            return {"key" : keyDecoder(jmsgAndMetadata[0][0]),\
                    "value" : valueDecoder(jmsgAndMetadata[0][1]),\
                    "topic" : utf8_decoder(jmsgAndMetadata[1][0]), \
                    "partition" : int(utf8_decoder(jmsgAndMetadata[1][1][0])), \
                    "offset" : long(utf8_decoder(jmsgAndMetadata[1][1][1]))}            
            '''
        return stream.map(getMetadataAndDecode)

    @staticmethod
    def getOffsetRanges(rdd):
        scalaRdd = rdd._jrdd.rdd()
        offsetRangesArray = scalaRdd.offsetRanges()
        return [ OffsetRange(topic = offsetRange.topic(),
                             partition = offsetRange.partition(), 
                             fromOffset = offsetRange.fromOffset(), 
                             untilOffset = offsetRange.untilOffset())
                    for offsetRange in offsetRangesArray]
    @staticmethod
    def createDirectStreamJB(ssc, topics, kafkaParams, fromOffsets={},
                           keyDecoder=utf8_decoder, valueDecoder=utf8_decoder, offsetRangeForeach=None, addOffsetRange=False):
        """
        FIXME: temporary working placeholder
        :param offsetRangeForeach: if different to None, this function should be a function from a list of OffsetRange to None, and is applied to the OffsetRange
            list of each rdd
        :param addOffsetRange: if False (default) output records are of the shape (kafkaKey, kafkaValue); if True output records are of the shape (offsetRange, (kafkaKey, kafkaValue)) for offsetRange the OffsetRange value for the Spark partition for the record

        """
        if not isinstance(topics, list):
            raise TypeError("topics should be list")
        if not isinstance(kafkaParams, dict):
            raise TypeError("kafkaParams should be dict")

        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper")
            helper = helperClass.newInstance()

            jfromOffsets = dict([(k._jTopicAndPartition(helper),
                                  v) for (k, v) in fromOffsets.items()])
            jstream = helper.createDirectStream(ssc._jssc, kafkaParams, set(topics), jfromOffsets)
        except Py4JJavaError as e:
            if 'ClassNotFoundException' in str(e.java_exception):
                KafkaUtils._printErrorMsg(ssc.sparkContext)
            raise e

        # (kafkaKey, kafkaValue)
        ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())  
        stream = DStream(jstream, ssc, ser)
        def do_transform(rdd):
            '''
            use if (offsetRangeForeach is not None) or addOffsetRange
            '''
            def decode(records):
                return ( (keyDecoder(k), valueDecoder(v))  for (k, v) in records )

            def decodeWithOffsetRange(offsetRangesList, sparkPartition, records):
                return ((offsetRangesList[sparkPartition], (keyDecoder(k), valueDecoder(v))) 
                          for (k,v) in records)

            offsetRangesList = KafkaUtils.getOffsetRanges(rdd)
            if offsetRangeForeach is not None:
                offsetRangeForeach(offsetRangesList)

            return rdd.mapPartitionsWithIndex(lambda partitionIdx, records: decodeWithOffsetRange(offsetRangesList, partitionIdx, records) ) if addOffsetRange\
                   else rdd.map(decode)

        return stream.transform(do_transform)

    @staticmethod
    def createDirectStreamJ(ssc, topics, kafkaParams, fromOffsets={},
                           keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):
        """
        FIXME: temporary working placeholder
        """
        if not isinstance(topics, list):
            raise TypeError("topics should be list")
        if not isinstance(kafkaParams, dict):
            raise TypeError("kafkaParams should be dict")

        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper")
            helper = helperClass.newInstance()

            jfromOffsets = dict([(k._jTopicAndPartition(helper),
                                  v) for (k, v) in fromOffsets.items()])
            jstream = helper.createDirectStreamJ(ssc._jssc, kafkaParams, set(topics), jfromOffsets)
        except Py4JJavaError as e:
            if 'ClassNotFoundException' in str(e.java_exception):
                KafkaUtils._printErrorMsg(ssc.sparkContext)
            raise e

        # we need to use a Deserializer for entries ((key, message), (topic, (partition, offset)))
        ser = PairDeserializer(PairDeserializer(NoOpSerializer(), NoOpSerializer()),\
                               PairDeserializer(NoOpSerializer(), \
                                                PairDeserializer(NoOpSerializer(), NoOpSerializer()) 
                                                ) 
                               )
        stream = DStream(jstream, ssc, ser)
        def getMetadataAndDecode(jmsgAndMetadata):
            # Note hardcoding class name as this is @staticmethod
            # and not @classmethod
            return _MessageAndMetadata(
                    key = keyDecoder(jmsgAndMetadata[0][0]),
                    value = valueDecoder(jmsgAndMetadata[0][1]),
                    topic = utf8_decoder(jmsgAndMetadata[1][0]),
                    partition = int(utf8_decoder(jmsgAndMetadata[1][1][0])), 
                    offset = long(utf8_decoder(jmsgAndMetadata[1][1][1]))
                   )
            '''
            return {"key" : keyDecoder(jmsgAndMetadata[0][0]),\
                    "value" : valueDecoder(jmsgAndMetadata[0][1]),\
                    "topic" : utf8_decoder(jmsgAndMetadata[1][0]), \
                    "partition" : int(utf8_decoder(jmsgAndMetadata[1][1][0])), \
                    "offset" : long(utf8_decoder(jmsgAndMetadata[1][1][1]))}            
            '''

        def printOffsets(rdd):
            scalaRdd = rdd._jrdd.rdd()
            # type(offsetRangesArray): <class 'py4j.java_collections.JavaArray'> 
            offsetRangesArray = scalaRdd.offsetRanges()
            print 
            print 
            # FIXME: test without extra java-style getters from OffsetRange
            # TODO: create named tuple and add to dict
            for partition, offsetRange in enumerate(offsetRangesArray):
              print 'offset for RDD partition', partition
              print '\tfromOffset', offsetRange.fromOffset()
              print '\tuntilOffset', offsetRange.untilOffset()
              print '\tKafka partition', offsetRange.partition()
              print '\ttopic', offsetRange.topic()
            print 
            print

        def getMetadataAndOffsetsAndDecode(rdd):
            offsetRangesList = KafkaUtils.getOffsetRanges(rdd)
            decodedRDDWithMetadata = rdd.map(getMetadataAndDecode)
            decodedRDDWithMetadata.__dict__["offsetRanges"] = offsetRangesList
            return decodedRDDWithMetadata

        # stream.foreachRDD(printOffsets)
        # return stream.map(getMetadataAndDecode)
        retStream = stream.transform(getMetadataAndOffsetsAndDecode)
        def p(rdd):
            print
            print 
            print "offsetRanges internal", rdd.__dict__.get("offsetRanges")
            print

        retStream.foreachRDD(p)
        return retStream

    @staticmethod
    def createRDD(sc, kafkaParams, offsetRanges, leaders={},
                  keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):
        """
        .. note:: Experimental

        Create a RDD from Kafka using offset ranges for each topic and partition.

        :param sc:  SparkContext object
        :param kafkaParams: Additional params for Kafka
        :param offsetRanges:  list of offsetRange to specify topic:partition:[start, end) to consume
        :param leaders: Kafka brokers for each TopicAndPartition in offsetRanges.  May be an empty
            map, in which case leaders will be looked up on the driver.
        :param keyDecoder:  A function used to decode key (default is utf8_decoder)
        :param valueDecoder:  A function used to decode value (default is utf8_decoder)
        :return: A RDD object
        """
        if not isinstance(kafkaParams, dict):
            raise TypeError("kafkaParams should be dict")
        if not isinstance(offsetRanges, list):
            raise TypeError("offsetRanges should be list")

        try:
            helperClass = sc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper")
            helper = helperClass.newInstance()
            joffsetRanges = [o._jOffsetRange(helper) for o in offsetRanges]
            jleaders = dict([(k._jTopicAndPartition(helper),
                              v._jBroker(helper)) for (k, v) in leaders.items()])
            jrdd = helper.createRDD(sc._jsc, kafkaParams, joffsetRanges, jleaders)
        except Py4JJavaError as e:
            if 'ClassNotFoundException' in str(e.java_exception):
                KafkaUtils._printErrorMsg(sc)
            raise e

        ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())
        rdd = RDD(jrdd, sc, ser)
        return rdd.map(lambda k_v: (keyDecoder(k_v[0]), valueDecoder(k_v[1])))

    @staticmethod
    def _printErrorMsg(sc):
        print("""
________________________________________________________________________________________________

  Spark Streaming's Kafka libraries not found in class path. Try one of the following.

  1. Include the Kafka library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages org.apache.spark:spark-streaming-kafka:%s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = org.apache.spark, Artifact Id = spark-streaming-kafka-assembly, Version = %s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <spark-streaming-kafka-assembly.jar> ...

________________________________________________________________________________________________

""" % (sc.version, sc.version))


class OffsetRange(object):
    """
    Represents a range of offsets from a single Kafka TopicAndPartition.
    """

    def __init__(self, topic, partition, fromOffset, untilOffset):
        """
        Create a OffsetRange to represent  range of offsets
        :param topic: Kafka topic name.
        :param partition: Kafka partition id.
        :param fromOffset: Inclusive starting offset.
        :param untilOffset: Exclusive ending offset.
        """
        self._topic = topic
        self._partition = partition
        self._fromOffset = fromOffset
        self._untilOffset = untilOffset

    def __str__(self):
        return "OffsetRange(topic={topic}, partition={partition}, fromOffset={fromOffset}, untilOffset={untilOffset})".format(topic=self._topic, partition=self._partition, fromOffset=self._fromOffset, untilOffset=self._untilOffset)

    def _jOffsetRange(self, helper):
        return helper.createOffsetRange(self._topic, self._partition, self._fromOffset,
                                        self._untilOffset)


class TopicAndPartition(object):
    """
    Represents a specific top and partition for Kafka.
    """

    def __init__(self, topic, partition):
        """
        Create a Python TopicAndPartition to map to the Java related object
        :param topic: Kafka topic name.
        :param partition: Kafka partition id.
        """
        self._topic = topic
        self._partition = partition

    def _jTopicAndPartition(self, helper):
        return helper.createTopicAndPartition(self._topic, self._partition)


class Broker(object):
    """
    Represent the host and port info for a Kafka broker.
    """

    def __init__(self, host, port):
        """
        Create a Python Broker to map to the Java related object.
        :param host: Broker's hostname.
        :param port: Broker's port.
        """
        self._host = host
        self._port = port

    def _jBroker(self, helper):
        return helper.createBroker(self._host, self._port)


/*
 * Copyright 2017-2018 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.opentracing.contrib.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.function.BiFunction;

public class ClientSpanNameProvider {

    // Operation Name as Span Name

    public static BiFunction<String, ConsumerRecord, String> CONSUMER_OPERATION_NAME =
            (operationName, consumerRecord) -> ((operationName == null) ? "unknown" : operationName);
    public static BiFunction<String, ProducerRecord, String> PRODUCER_OPERATION_NAME =
            (operationName, producerRecord) -> ((operationName == null) ? "unknown" : operationName);

    public static BiFunction<String, ConsumerRecord, String> CONSUMER_PREFIXED_OPERATION_NAME(final String prefix) {
        return (operationName, consumerRecord) -> ((prefix == null) ? "" : prefix)
                + ((operationName == null) ? "unknown" : operationName);
    }
    public static BiFunction<String, ProducerRecord, String> PRODUCER_PREFIXED_OPERATION_NAME(final String prefix) {
        return (operationName, producerRecord) -> ((prefix == null) ? "" : prefix)
                + ((operationName == null) ? "unknown" : operationName);
    }

    // Topic as Span Name
    public static BiFunction<String, ConsumerRecord, String> CONSUMER_TOPIC =
            (operationName, consumerRecord) -> ((consumerRecord == null) ? "unknown" : consumerRecord.topic());
    public static BiFunction<String, ProducerRecord, String> PRODUCER_TOPIC =
            (operationName, producerRecord) -> ((producerRecord == null) ? "unknown" : producerRecord.topic());

    public static BiFunction<String, ConsumerRecord, String> CONSUMER_PREFIXED_TOPIC(final String prefix) {
        return (operationName, consumerRecord) -> ((prefix == null) ? "" : prefix)
                + ((consumerRecord == null) ? "unknown" : consumerRecord.topic());
    }
    public static BiFunction<String, ProducerRecord, String> PRODUCER_PREFIXED_TOPIC(final String prefix) {
        return (operationName, producerRecord) -> ((prefix == null) ? "" : prefix)
                + ((producerRecord == null) ? "unknown" : producerRecord.topic());
    }

    // Operation Name and Topic as Span Name
    public static BiFunction<String, ConsumerRecord, String> CONSUMER_OPERATION_NAME_TOPIC =
            (operationName, consumerRecord) -> ((operationName == null) ? "unknown" : operationName)
            + " - " + ((consumerRecord == null) ? "unknown" : consumerRecord.topic());
    public static BiFunction<String, ProducerRecord, String> PRODUCER_OPERATION_NAME_TOPIC =
            (operationName, producerRecord) -> ((operationName == null) ? "unknown" : operationName)
            + " - " + ((producerRecord == null) ? "unknown" : producerRecord.topic());

    public static BiFunction<String, ConsumerRecord, String> CONSUMER_PREFIXED_OPERATION_NAME_TOPIC(final String prefix) {
        return (operationName, consumerRecord) -> ((prefix == null) ? "" : prefix)
                + ((operationName == null) ? "unknown" : operationName)
                + " - " + ((consumerRecord == null) ? "unknown" : consumerRecord.topic());
    }
    public static BiFunction<String, ProducerRecord, String> PRODUCER_PREFIXED_OPERATION_NAME_TOPIC(final String prefix) {
        return (operationName, producerRecord) -> ((prefix == null) ? "" : prefix)
                + ((operationName == null) ? "unknown" : operationName)
                + " - " + ((producerRecord == null) ? "unknown" : producerRecord.topic());
    }

}

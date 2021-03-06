/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.writable.kryo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Generic wrapper object, making any object writable.
 *
 * Uses Kryo inside for serialization.
 * Current configuration is not optimized for performance,
 * but Writable interface doesn't allow much room for it.
 *
 * Note - Java8 lambdas need to implement Serializable to work.
 *
 * @param <T> Object type
 */
public class KryoWritableWrapper<T> implements Writable {
  /** Wrapped object */
  private T object;

  /**
   * Create wrapper given an object.
   * @param object Object instance
   */
  public KryoWritableWrapper(T object) {
    this.object = object;
  }

  /**
   * Creates wrapper initialized with null.
   */
  public KryoWritableWrapper() {
  }

  /**
   * Unwrap the object value
   * @return Object value
   */
  public T get() {
    return object;
  }

  /**
   * Set wrapped object value
   * @param object New object value
   */
  public void set(T object) {
    this.object = object;
  }

  @Override
  public void readFields(DataInput in) throws java.io.IOException {
    object = HadoopKryo.readClassAndObject(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    HadoopKryo.writeClassAndObject(out, object);
  }

  /**
   * Returns Writable instance, wrapping given object only
   * if it is not already writable.
   *
   * @param object Object to potentially wrap
   * @return Writable object holding argument
   */
  public static Writable wrapIfNeeded(Object object) {
    if (object instanceof Writable) {
      return (Writable) object;
    } else {
      return new KryoWritableWrapper<>(object);
    }
  }

  /**
   * Unwrap Writable object if it was wrapped initially,
   * inverse of wrapIfNeeded function.
   * @param value Potentially wrapped value
   * @return Original unwrapped value
   * @param <T> Type of returned object.
   */
  public static <T> T unwrapIfNeeded(Writable value) {
    if (value instanceof KryoWritableWrapper) {
      return ((KryoWritableWrapper<T>) value).get();
    } else {
      return (T) value;
    }
  }
}

/**
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
package org.apache.hadoop.hive.serde2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * DateWritable
 * Writable equivalent of java.sql.Date
 *
 * Dates are of the format
 *    YYYY-MM-DD
 *
 */
public class DateWritable implements WritableComparable<DateWritable> {
  static final private Log LOG = LogFactory.getLog(DateWritable.class);

  private Date date = new Date(0);

  /* Constructors */
  public DateWritable() {
  }

  public DateWritable(DateWritable d) {
    set(d);
  }

  public DateWritable(Date d) {
    set(d);
  }

  public void set(Date d) {
    if (d == null) {
      date.setTime(0);
      return;
    }
    this.date = d;
  }

  public void set(DateWritable d) {
    set(d.get());
  }

  public Date get() {
     return date;
  }

  /**
   *
   * @return time in seconds corresponding to this DateWritable
   */
  public long getTimeInSeconds() {
    return date.getTime() / 1000;
  }

  public static Date timeToDate (long l) {
    return new Date(l * 1000);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    date.setTime(WritableUtils.readVLong(in) * 1000);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVLong(out, getTimeInSeconds());
  }

  @Override
  public int compareTo(DateWritable d) {
    long diff = date.getTime() - d.get().getTime();
    if (diff > 0) {
      return 1;
    } else if (diff == 0) {
      return 0;
    } else {
      return -1;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DateWritable)) {
      return false;
    }
    return compareTo((DateWritable) o) == 0;
  }

  @Override
  public String toString() {
    return date.toString();
  }

  @Override
  public int hashCode() {
    return date.hashCode();
  }
}

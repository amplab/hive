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

package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class DummyStorageHandler extends DefaultStorageHandler implements HiveMetaHook {
  public static class NullInputFormat implements InputFormat<BytesWritable, BytesWritable> {
    @Override
    public RecordReader<BytesWritable, BytesWritable> getRecordReader(InputSplit arg0, JobConf arg1,
        Reporter arg2) throws IOException {
      return new RecordReader<BytesWritable, BytesWritable>() {
        @Override
        public void close() throws IOException {
        }

        @Override
        public BytesWritable createKey() {
          return new BytesWritable();
        }

        @Override
        public BytesWritable createValue() {
          return new BytesWritable();
        }

        @Override
        public long getPos() throws IOException {
          return 0;
        }

        @Override
        public float getProgress() throws IOException {
          return 0;
        }

        @Override
        public boolean next(BytesWritable arg0, BytesWritable arg1) throws IOException {
          return false;
        }
      };
    }

    @Override
    public InputSplit[] getSplits(JobConf arg0, int arg1) throws IOException {
      return new InputSplit[0];
    }
  }

  public static class NullOutputFormat implements OutputFormat<BytesWritable, BytesWritable>, HiveOutputFormat<BytesWritable, BytesWritable> {
    @Override
    public void checkOutputSpecs(FileSystem arg0, JobConf arg1) throws IOException {
    }

    @Override
    public RecordWriter<BytesWritable, BytesWritable> getRecordWriter(FileSystem arg0, JobConf arg1,
        String arg2, Progressable arg3) throws IOException {
      return new RecordWriter<BytesWritable, BytesWritable>() {
        @Override
        public void close(Reporter arg0) throws IOException {
        }

        @Override
        public void write(BytesWritable arg0, BytesWritable arg1) throws IOException {
        }
      };
    }

    @Override
    public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
        JobConf jc, Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed,
        Properties tableProperties, Progressable progress) throws IOException {
      return new org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter() {
        @Override
        public void write(Writable w) throws IOException {
        }

        @Override
        public void close(boolean abort) throws IOException {
        }
      };
    }
  }

  public static class NullSerDe extends LazyBinaryColumnarSerDe {
    public NullSerDe() throws SerDeException {
      super();
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
      return null;
    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
      return null;
    }
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return NullInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return NullOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return NullSerDe.class;
  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    table.getSd().setOutputFormat(NullOutputFormat.class.getCanonicalName());
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
  }
}

package org.apache.pinot.util;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


public class MockRecordReader implements RecordReader {
  private final List<GenericRow> _rows;
  private final AtomicBoolean _hasConsumed = new AtomicBoolean();
  private Iterator<GenericRow> _rowIterator;

  public MockRecordReader(List<GenericRow> rows) {
    _rows = rows;
    _hasConsumed.set(false);
    _rowIterator = _rows.iterator();
  }

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
  }

  @Override
  public boolean hasNext() {
    return _rowIterator.hasNext();
  }

  @Override
  public GenericRow next()
      throws IOException {
    _hasConsumed.set(true);
    return _rowIterator.next();
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    _hasConsumed.set(true);
    reuse.clear();
    reuse.init(_rowIterator.next());
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _rowIterator = _rows.iterator();
  }

  @Override
  public void close()
      throws IOException {
  }
}

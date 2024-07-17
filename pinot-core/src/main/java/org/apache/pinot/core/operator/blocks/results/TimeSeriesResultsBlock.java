package org.apache.pinot.core.operator.blocks.results;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.tsdb.spi.series.SeriesBlock;


public class TimeSeriesResultsBlock extends BaseResultsBlock {
  private final SeriesBlock _seriesBlock;

  public TimeSeriesResultsBlock(SeriesBlock seriesBlock) {
    _seriesBlock = seriesBlock;
  }

  @Override
  public int getNumRows() {
    return 0;
  }

  @Nullable
  @Override
  public QueryContext getQueryContext() {
    return null;
  }

  @Nullable
  @Override
  public DataSchema getDataSchema() {
    return null;
  }

  @Nullable
  @Override
  public List<Object[]> getRows() {
    return List.of();
  }

  @Override
  public DataTable getDataTable()
      throws IOException {
    return null;
  }

  public SeriesBlock getSeriesBlock() {
    return _seriesBlock;
  }
}

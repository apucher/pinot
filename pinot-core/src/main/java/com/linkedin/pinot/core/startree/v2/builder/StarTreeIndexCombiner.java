/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.startree.v2.builder;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.startree.v2.AggregationFunctionColumnPair;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;

import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.Indexes.*;
import static com.linkedin.pinot.core.startree.v2.StarTreeV2Constants.*;
import static com.linkedin.pinot.core.startree.v2.store.StarTreeIndexMapUtils.*;


/**
 * The {@code StarTreeIndexCombiner} class combines multiple star-tree indexes into a single index file.
 */
public class StarTreeIndexCombiner implements Closeable {
  private final FileChannel _fileChannel;

  public StarTreeIndexCombiner(File indexFile) throws IOException {
    Preconditions.checkState(!indexFile.exists(), "Star-tree index file already exists");
    _fileChannel = new RandomAccessFile(indexFile, "rw").getChannel();
  }

  /**
   * Combines the index files inside the given directory into the single index file, then cleans the directory.
   */
  public Map<IndexKey, IndexValue> combine(StarTreeV2BuilderConfig builderConfig, File starTreeIndexDir)
      throws IOException {
    Map<IndexKey, IndexValue> indexMap = new HashMap<>();

    // Write star-tree index
    File starTreeIndexFile = new File(starTreeIndexDir, STAR_TREE_INDEX_FILE_NAME);
    indexMap.put(STAR_TREE_INDEX_KEY, writeFile(starTreeIndexFile));

    // Write dimension indexes
    for (String dimension : builderConfig.getDimensionsSplitOrder()) {
      File dimensionIndexFile = new File(starTreeIndexDir, dimension + UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
      indexMap.put(new IndexKey(IndexType.FORWARD_INDEX, dimension), writeFile(dimensionIndexFile));
    }

    // Write metric (function-column pair) indexes
    for (AggregationFunctionColumnPair functionColumnPair : builderConfig.getFunctionColumnPairs()) {
      String metric = functionColumnPair.toColumnName();
      File metricIndexFile = new File(starTreeIndexDir, metric + RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
      indexMap.put(new IndexKey(IndexType.FORWARD_INDEX, metric), writeFile(metricIndexFile));
    }

    FileUtils.cleanDirectory(starTreeIndexDir);
    return indexMap;
  }

  private IndexValue writeFile(File srcFile) throws IOException {
    try (FileChannel src = new RandomAccessFile(srcFile, "r").getChannel()) {
      long offset = _fileChannel.position();
      long size = src.size();
      com.linkedin.pinot.common.utils.FileUtils.transferBytes(src, 0, size, _fileChannel);
      return new IndexValue(offset, size);
    }
  }

  @Override
  public void close() throws IOException {
    _fileChannel.close();
  }
}

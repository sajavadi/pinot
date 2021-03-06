/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.blocks;

import org.roaringbitmap.buffer.MutableRoaringBitmap;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnMultiValueReaderWriter;
import com.linkedin.pinot.core.operator.docvalsets.RealtimeMultiValueSet;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionary;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class RealtimeMultiValueBlock implements Block {

  private final MutableRoaringBitmap filteredBitmap;
  final FieldSpec spec;
  private final MutableDictionary dictionary;
  final int docIdSearchableOffset;
  final FixedByteSingleColumnMultiValueReaderWriter reader;
  private Predicate p;
  private final int maxNumberOfMultiValues;

  public RealtimeMultiValueBlock(FieldSpec spec, MutableDictionary dictionary, MutableRoaringBitmap filteredDocids,
      int docIdOffset, int maxNumberOfMultiValues, FixedByteSingleColumnMultiValueReaderWriter indexReader) {
    this.spec = spec;
    this.dictionary = dictionary;
    this.filteredBitmap = filteredDocids;
    this.docIdSearchableOffset = docIdOffset;
    this.maxNumberOfMultiValues = maxNumberOfMultiValues;
    this.reader = indexReader;
  }

  @Override
  public BlockId getId() {
    return null;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    this.p = predicate;
    return true;
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    if (this.p != null) {
      return BlockFactory.getBlockDocIdSetBackedByBitmap(filteredBitmap);
    }

    return BlockFactory.getDummyBlockDocIdSet(docIdSearchableOffset);
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return new RealtimeMultiValueSet(reader, docIdSearchableOffset + 1, spec.getDataType());
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    return null;
  }

  @Override
  public BlockMetadata getMetadata() {
    return new BlockMetadata() {

      @Override
      public boolean isSparse() {
        return false;
      }

      @Override
      public boolean isSorted() {
        return false;
      }

      @Override
      public boolean isSingleValue() {
        return false;
      }

      @Override
      public boolean hasInvertedIndex() {
        return true;
      }

      @Override
      public boolean hasDictionary() {
        return true;
      }

      @Override
      public int getStartDocId() {
        return 0;
      }

      @Override
      public int getSize() {
        return docIdSearchableOffset + 1;
      }

      @Override
      public int getLength() {
        return docIdSearchableOffset + 1;
      }

      @Override
      public int getEndDocId() {
        return docIdSearchableOffset;
      }

      @Override
      public Dictionary getDictionary() {
        return dictionary;
      }

      @Override
      public DataType getDataType() {
        return spec.getDataType();
      }

      @Override
      public int getMaxNumberOfMultiValues() {
        return maxNumberOfMultiValues;
      }
    };
  }
}

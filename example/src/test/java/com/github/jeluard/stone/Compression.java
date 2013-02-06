/*
 * Copyright 2013 julien.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jeluard.stone;

import com.github.jeluard.stone.storage.memory.MemoryStorage;
import com.kamikaze.docidset.api.DocSet;
import com.kamikaze.docidset.utils.DocSetFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.Deflater;
import lzma.sdk.lzma.Encoder;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;
import org.iq80.snappy.Snappy;

/**
 *
 * @author julien
 */
public class Compression {
  public static void main(String[] args) throws Exception {
    
    
    MemoryStorage storage = new MemoryStorage(10000, 1);
    int[] consolidates = new int[] {1};
    while (!Thread.currentThread().isInterrupted()) {
      long before = System.currentTimeMillis();
      for (int i = 0; i < 1000*1000; i++) {
        storage.onConsolidation(before, consolidates);
      }
      System.out.println("time: "+(System.currentTimeMillis()-before));
    }
    
    System.exit(0);
    
    
    ByteBuffer buffer = ByteBuffer.allocate(500);
    final Random random = new Random();
    for (int i = 0; i < 25; i++) {
      buffer.putLong(System.currentTimeMillis());
      buffer.putInt(random.nextInt(100));
      buffer.putInt(random.nextInt(100));
      buffer.putInt(random.nextInt(100));
    }

    byte[] array = buffer.array();
    LZ4Factory factory = LZ4Factory.unsafeInstance();
    LZ4Compressor compressor = factory.highCompressor();
    int decompressedLength = array.length;
    int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
    byte[] compressed = new byte[maxCompressedLength];
    int compressedLength = compressor.compress(array, 0, decompressedLength, compressed, 0, maxCompressedLength);

    System.out.println("before: "+array.length);
    System.out.println("after lz4: "+compressedLength);

    
    int length = Snappy.compress(array, 0, array.length, new byte[1000], 0);

    System.out.println("after snappy: "+length);
    
    //http://www.java-examples.com/compress-byte-array-using-deflater-example
    
    long before = System.currentTimeMillis();
    byte[] output = new byte[1000];
     Deflater compresser = new Deflater(Deflater.BEST_COMPRESSION);
     compresser.setInput(array);
     compresser.finish();
     int compressedDataLength = compresser.deflate(output);
     compresser.end();
     long after = System.currentTimeMillis();
     
    System.out.println("after deflate: "+compressedDataLength+" in ("+(after-before+") ms"));

    final Encoder encoder = new Encoder();
    before = System.currentTimeMillis();
    ByteArrayInputStream inStream = new ByteArrayInputStream(array);
    ByteArrayOutputStream outStream = new ByteArrayOutputStream(150);
    encoder.code(inStream, outStream, 0, 0, null);
    after = System.currentTimeMillis();
    
    System.out.println("after lzma: "+outStream.toByteArray().length+" in ("+(after-before+") ms"));

    LZ4Decompressor decompressor = factory.decompressor();
    byte[] restored = new byte[decompressedLength];
    int compressedLength2 = decompressor.decompress(compressed, 0, restored, 0, decompressedLength);

    DocSet pForDeltaDocSet = DocSetFactory.getPForDeltaDocSetInstance(); 
    for(int i = 0; i< 27; i++) {         
      pForDeltaDocSet.addDoc(random.nextInt(100));
    }
    pForDeltaDocSet.optimize();
    System.out.println("bytes: "+pForDeltaDocSet.sizeInBytes());
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    new ObjectOutputStream(outputStream).writeObject(pForDeltaDocSet);
    System.out.println("length: "+outputStream.size());
  }
}

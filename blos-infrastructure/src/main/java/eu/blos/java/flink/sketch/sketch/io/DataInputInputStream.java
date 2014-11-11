package eu.blos.java.flink.sketch.sketch.io;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

public class DataInputInputStream extends InputStream {

  private DataInput in;

  /**
   * Construct an InputStream from the given DataInput. If 'in'
   * is already an InputStream, simply returns it. Otherwise, wraps
   * it in an InputStream.
   * @param in the DataInput to wrap
   * @return an InputStream instance that reads from 'in'
   */
  public static InputStream constructInputStream(DataInput in) {
    if (in instanceof InputStream) {
      return (InputStream)in;
    } else {
      return new DataInputInputStream(in);
    }
  }


  public DataInputInputStream(DataInput in) {
    this.in = in;
  }

  @Override
  public int read() throws IOException {
    return in.readUnsignedByte();
  }
}
package com.bytezone.dm3270.utilities;

import java.io.UnsupportedEncodingException;
import java.util.Optional;

import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.ButtonType;

public class Dm3270Utility
{
  public static final String EBCDIC = "CP1047";
  public static final String FROG = "CP1047";
  private static final int LINESIZE = 16;

  public static final int[] ebc2asc = new int[256];
  public static final int[] asc2ebc = new int[256];

  static
  {
    byte[] values = new byte[256];
    for (int i = 0; i < 256; i++)
      values[i] = (byte) i;

    try
    {
      String s = new String (values, EBCDIC);
      char[] chars = s.toCharArray ();
      for (int i = 0; i < 256; i++)
      {
        int val = chars[i];
        ebc2asc[i] = val;
        asc2ebc[val] = i;
      }
    }
    catch (UnsupportedEncodingException e)
    {
      e.printStackTrace ();
    }
  }

  public static String ebc2asc (byte[] buffer)
  {
    byte[] newBuffer = new byte[buffer.length];
    int ptr = 0;
    for (int i = 0; i < buffer.length; i++)
      if (buffer[i] != 0)                                       // suppress nulls
        newBuffer[ptr++] = (byte) ebc2asc[buffer[i] & 0xFF];

    return new String (newBuffer);
  }

  public static String getString (byte[] buffer)
  {
    return getString (buffer, 0, buffer.length);
  }

  public static String getString (byte[] buffer, int offset, int length)
  {
    try
    {
      if (offset + length > buffer.length)
        length = buffer.length - offset - 1;
      return new String (buffer, offset, length, EBCDIC);
    }
    catch (UnsupportedEncodingException e)
    {
      e.printStackTrace ();
      return "FAIL";
    }
  }

  public static String getSanitisedString (byte[] buffer, int offset, int length)
  {
    if (offset + length > buffer.length)
      length = buffer.length - offset - 1;
    return getString (sanitise (buffer, offset, length));
  }

  private static byte[] sanitise (byte[] buffer, int offset, int length)
  {
    byte[] cleanBuffer = new byte[length];
    for (int i = 0; i < length; i++)
    {
      int b = buffer[offset++] & 0xFF;
      cleanBuffer[i] = b < 0x40 ? 0x40 : (byte) b;
    }
    return cleanBuffer;
  }

  public static int unsignedShort (byte[] buffer, int offset)
  {
    return (buffer[offset] & 0xFF) * 0x100 + (buffer[offset + 1] & 0xFF);
  }

  public static int packUnsignedShort (int value, byte[] buffer, int offset)
  {
    buffer[offset++] = (byte) ((value >> 8) & 0xFF);
    buffer[offset++] = (byte) (value & 0xFF);

    return offset;
  }

  public static int unsignedLong (byte[] buffer, int offset)
  {
    return (buffer[offset] & 0xFF) * 0x1000000 + (buffer[offset + 1] & 0xFF) * 0x10000
        + (buffer[offset + 2] & 0xFF) * 0x100 + (buffer[offset + 3] & 0xFF);
  }

  public static int packUnsignedLong (long value, byte[] buffer, int offset)
  {
    buffer[offset++] = (byte) ((value >> 24) & 0xFF);
    buffer[offset++] = (byte) ((value >> 16) & 0xFF);
    buffer[offset++] = (byte) ((value >> 8) & 0xFF);
    buffer[offset++] = (byte) (value & 0xFF);

    return offset;
  }

  public static String toHex (byte[] b)
  {
    return toHex (b, 0, b.length);
  }

  public static String toHex (byte[] b, int offset)
  {
    return toHex (b, offset, b.length - offset);
  }

  public static String toHex (byte[] b, int offset, int length)
  {
    return toHex (b, offset, length, true);
  }

  public static String toHex (byte[] b, boolean ebcdic)
  {
    return toHex (b, 0, b.length, ebcdic);
  }

  public static String toHex (byte[] b, int offset, int length, boolean ebcdic)
  {
    StringBuilder text = new StringBuilder ();

    try
    {
      for (int ptr = offset, max = offset + length; ptr < max; ptr += LINESIZE)
      {
        final StringBuilder hexLine = new StringBuilder ();
        final StringBuilder textLine = new StringBuilder ();
        for (int linePtr = 0; linePtr < LINESIZE; linePtr++)
        {
          if (ptr + linePtr >= max)
            break;

          int val = b[ptr + linePtr] & 0xFF;
          hexLine.append (String.format ("%02X ", val));

          if (ebcdic)
            if (val < 0x40 || val == 0xFF)
              textLine.append ('.');
            else
              textLine.append (new String (b, ptr + linePtr, 1, EBCDIC));
          else if (val < 0x20 || val >= 0xF0)
            textLine.append ('.');
          else
            textLine.append (new String (b, ptr + linePtr, 1));
        }
        text.append (String.format ("%04X  %-48s %s%n", ptr, hexLine.toString (),
                                    textLine.toString ()));
      }
    }
    catch (UnsupportedEncodingException e)
    {
      e.printStackTrace ();
    }

    if (text.length () > 0)
      text.deleteCharAt (text.length () - 1);

    return text.toString ();
  }

  public static void hexDump (byte[] b, boolean ebcdic)
  {
    hexDump (b, 0, b.length, ebcdic);
  }

  public static void hexDump (byte[] b)
  {
    hexDump (b, 0, b.length);
  }

  public static void hexDump (byte[] b, int offset, int length)
  {
    System.out.println (toHex (b, offset, length));
  }

  public static void hexDump (byte[] b, int offset, int length, boolean ebcdic)
  {
    System.out.println (toHex (b, offset, length, ebcdic));
  }

  public static String toHexString (byte[] buffer)
  {
    StringBuilder text = new StringBuilder ();
    for (int i = 0; i < buffer.length; i++)
      text.append (String.format ("%02X ", buffer[i]));
    return text.toString ();
  }

  public static String toHexString (byte[] buffer, int offset, int length)
  {
    StringBuilder text = new StringBuilder ();
    for (int ptr = offset, max = offset + length; ptr < max; ptr++)
      text.append (String.format ("%02X ", (buffer[ptr] & 0xFF)));
    if (text.length () > 0)
      text.deleteCharAt (text.length () - 1);
    return text.toString ();
  }

  public static void printStackTrace ()
  {
    for (StackTraceElement ste : Thread.currentThread ().getStackTrace ())
      System.out.println (ste);
  }

  public static boolean showAlert (String message)
  {
    Alert alert = new Alert (AlertType.ERROR, message);
    alert.getDialogPane ().setHeaderText (null);
    Optional<ButtonType> result = alert.showAndWait ();
    return (result.isPresent () && result.get () == ButtonType.OK);
  }
}
